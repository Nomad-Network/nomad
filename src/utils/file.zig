const std = @import("std");

const ReadErr = error{ FileOpenErr, ReadErr, StatFileErr };

const FileExistsOptions = struct {
    create_if_not_exist: ?bool,
};

pub fn readFile(allocator: std.mem.Allocator, path: []const u8) ![]const u8 {
    var lines = std.ArrayList([]const u8).init(allocator);
    defer lines.deinit();

    var file = std.fs.cwd().openFile(path, .{}) catch return error.FileOpenErr;
    defer file.close();

    const file_stat = std.fs.cwd().statFile(path) catch return error.StatFileErr;
    const buffer = allocator.alloc(u8, file_stat.size) catch return error.ReadErr;

    _ = file.readAll(buffer) catch return error.ReadErr;

    return buffer;
}

pub fn fileExists(path: []const u8, options: FileExistsOptions) bool {
    _ = std.fs.cwd().createFile(path, .{ .exclusive = true }) catch |e|
        switch (e) {
        error.PathAlreadyExists => {
            return true;
        },
        else => @panic(@errorName(e)),
    };

    if (!(options.create_if_not_exist orelse false)) {
        errdefer std.fs.cwd().deleteFile(path);
    }

    return false;
}

pub fn writeFile(path: []const u8, content: []u8) !void {
    var file = std.fs.cwd().openFile(path, .{ .mode = .write_only }) catch return error.FileOpenErr;
    defer file.close();

    try file.writeAll(content);
}
