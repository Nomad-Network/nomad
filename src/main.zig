const std = @import("std");
const clap = @import("clap");

const nomad = @import("./nomad.zig");
const file_utils = @import("./utils/file.zig");
const mem_utils = @import("./utils/mem.zig");

const Context = struct {
    serialized_data: ?[]u8 = null,
    db_handle: ?*nomad.Database = null,
    record: ?*nomad.Record = null,
};

var ctx = Context{};

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
        var record = nomad.Record.init(allocator, std.mem.zeroes([2048]u8), null, null);
        record.last_ping = 0x1000100010001000;
        try db.addRecord(&record);
        ctx.serialized_data = try db.serialize();
        ctx.db_handle = &db;
        ctx.record = &record;

        var queue = nomad.Queue.init(allocator, "TestQueue");
        const Callbacks = struct {
            fn testFn() nomad.Queue.Status {
                std.log.info("Run: {s}", .{""});
                return .done;
            }

            fn logData() nomad.Queue.Status {
                if (ctx.db_handle) |dbh| {
                    dbh.commit() catch return .failed;
                    dbh.print() catch return .failed;
                    const hash = ctx.record.?.hash() catch return .failed;
                    const db_record = dbh.getRecord(hash) catch return .failed;
                    std.debug.print("ping: 0x{X:0>16} hash: 0x{X:0>16}\n", .{ db_record.last_ping, hash });
                }

                return .done;
            }
        };

        try queue.tasks.enqueue(.{
            .name = "A test function",
            .method = Callbacks.testFn,
        });

        try queue.tasks.enqueue(.{
            .name = "Log Nomad Data",
            .method = Callbacks.logData,
        });

        try queue.start();
    }
}

test "simple test" {
    var list = std.ArrayList(i32).init(std.testing.allocator);
    defer list.deinit(); // try commenting this out and see if zig detects the memory leak!
    try list.append(42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}
