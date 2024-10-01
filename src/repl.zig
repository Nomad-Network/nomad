const std = @import("std");
const clap = @import("clap");

const nomad = @import("./nomad.zig");
const file_utils = @import("./utils/file.zig");
const mem_utils = @import("./utils/mem.zig");

const ReplQueue = nomad.Queue(Context);

const Context = struct {
    db_handle: ?*nomad.Database = null,
    queue: ?*ReplQueue = null,
};

const Callbacks = struct {
    fn testFn(_: *Context) ReplQueue.Status {
        return .done;
    }

    fn logData(ctx: *Context) ReplQueue.Status {
        if (ctx.db_handle) |dbh| {
            dbh.commit() catch return .failed;
            dbh.print() catch return .failed;
        }

        return .done;
    }
};

fn processCommand(alloc: std.mem.Allocator, instruction: []u8) ![][]u8 {
    var tmp_var = std.ArrayList(u8).init(alloc);
    var out = std.ArrayList([]u8).init(alloc);

    var should_process = true;
    var should_escape = false;
    var nesting: u64 = 0;

    for (instruction) |char| {
        if (char == '"') should_process = !should_process;
        if (char == '(') nesting += 1;
        if (char == ')') nesting -= 1;
        if (char == '\\') should_escape = true;

        if (should_escape) {
            try tmp_var.append(char);
            should_escape = false;
        }

        if (should_process and nesting == 0 and char == ' ') {
            const copy = try alloc.alloc(u8, tmp_var.items.len);
            std.mem.copyForwards(u8, copy, tmp_var.items);

            try out.append(@constCast(std.mem.trimLeft(u8, copy, " ")));

            tmp_var.clearAndFree();
        }

        try tmp_var.append(char);
    }

    if (tmp_var.items.len > 0) {
        const copy = try alloc.alloc(u8, tmp_var.items.len);
        std.mem.copyForwards(u8, copy, tmp_var.items);

        try out.append(@constCast(std.mem.trimLeft(u8, copy, " ")));
    }

    return out.items;
}

fn stringToCString(allocator: std.mem.Allocator, str: []u8) ![:0]u8 {
    const n = try allocator.allocSentinel(u8, str.len + 1, 0);
    std.mem.copyForwards(u8, n, str);
    n[n.len - 1] = 0;

    return n;
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var arena = std.heap.ArenaAllocator.init(gpa.allocator());
    defer arena.deinit();

    const allocator = arena.allocator();

    // First we specify what parameters our program can take.
    // We can use `parseParamsComptime` to parse a string into an array of `Param(Help)`
    const params = comptime clap.parseParamsComptime(
        \\-h, --help             Display this help and exit.
        \\-f, --file <str>       The nomad data file to interact with
        \\-p, --port <u16>       The port to serve on
        \\
    );

    // Initialize our diagnostics, which can be used for reporting useful errors.
    // This is optional. You can also pass `.{}` to `clap.parse` if you don't
    // care about the extra information `Diagnostics` provides.
    var diag = clap.Diagnostic{};
    var res = clap.parse(clap.Help, &params, clap.parsers.default, .{
        .diagnostic = &diag,
        .allocator = allocator,
    }) catch |err| {
        // Report useful error and exit
        diag.report(std.io.getStdErr().writer(), err) catch {};
        return err;
    };
    defer res.deinit();

    if (res.args.help != 0) {
        try clap.help(std.io.getStdErr().writer(), clap.Help, &params, .{});
    } else if (res.args.file) |file| {
        var db = try nomad.Database.init(allocator, file);
        var ctx = Context{};
        ctx.db_handle = &db;

        var queue = ReplQueue.init(allocator, "TestQueue", ctx);
        queue.logging_enabled = false;

        ctx.queue = &queue;

        try queue.start();

        var stdin = std.io.getStdIn().reader();

        var prev_cmd: []u8 = "";

        while (!std.mem.eql(u8, prev_cmd, "QUIT")) {
            std.debug.print("NOMAD {s} > ", .{prev_cmd});
            const input = try stdin.readUntilDelimiterAlloc(allocator, '\n', 0xff);
            const cmd_parts = try processCommand(allocator, input);

            if (cmd_parts.len > 0) {
                prev_cmd = cmd_parts[0];
            } else {
                continue;
            }

            if (std.mem.eql(u8, cmd_parts[0], "INSERT")) {
                const data = cmd_parts[1];

                var record = try nomad.Record.init(null, null);
                try record.setData(data);

                std.log.info("STRING: {s} {s}", .{ data, record.data });
                const record_hash = try db.addRecord(record);
                std.log.info("record created: 0x{X:0>16}", .{record_hash});
            } else if (std.mem.eql(u8, cmd_parts[0], "COMMIT")) {
                try db.commit();
                std.log.info("COMMITED {d} RECORD(S)", .{db.records.items.len});
            } else if (std.mem.eql(u8, cmd_parts[0], "FETCH")) {
                const hash_string = cmd_parts[1];

                if (!std.mem.eql(u8, hash_string[0..2], "0x")) continue;

                const hash = try std.fmt.parseUnsigned(u64, hash_string, 0);

                std.log.debug("HASH: 0x{X:0>16}", .{hash});

                for (db.records.items, 0..) |r, i| {
                    std.log.debug("R{}: {any}", .{ i, r.data[0..8] });
                }

                const record = try db.getRecord(hash);

                std.debug.print("DATA: {any} {any}\n", .{ record.data.len, record.getData() });
            } else if (std.mem.eql(u8, cmd_parts[0], "PRINT")) {
                try db.print();
            }

            std.log.info("{s}", .{cmd_parts});
        }
    }
}

test "simple test" {
    var list = std.ArrayList(i32).init(std.testing.allocator);
    defer list.deinit(); // try commenting this out and see if zig detects the memory leak!
    try list.append(42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}
