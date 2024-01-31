const std = @import("std");

const utils = @import("../utils/file.zig");
const LookupTable = @import("./lookuptable.zig");
const Header = @import("./header.zig");
const Record = @import("./records.zig");

const Self = @This();

lookup_table: LookupTable,
records: std.ArrayList(Record),
deleted_records: std.ArrayList(Record),

pub fn init(allocator: std.mem.Allocator, file: []const u8) !Self {
    const exists = utils.fileExists(file, .{ .create_if_not_exist = true });

    if (!exists) {
        try utils.writeFile(file, try Header.default(allocator).serialize());
    }

    const content = try utils.readFile(allocator, file);
    const lookup_table = LookupTable.init(allocator, content);

    const records = std.ArrayList(Record).init(allocator);
    const deleted_records = std.ArrayList(Record).init(allocator);

    return Self{ .records = records, .lookup_table = lookup_table, .deleted_records = deleted_records };
}
