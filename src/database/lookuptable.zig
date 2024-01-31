const std = @import("std");
const utils = @import("../utils/file.zig");

const DataHeader = @import("./header.zig");

const Self = @This();

const InternalType = std.AutoHashMap(i128, i128);

header: DataHeader,
records_table: InternalType,
deleted_table: InternalType,
allocator: std.mem.Allocator,

pub fn init(allocator: std.mem.Allocator, content: []const u8) Self {
    const header = DataHeader.init(allocator, content);

    return Self{
        .header = header,
        .records_table = InternalType.init(allocator),
        .deleted_table = InternalType.init(allocator),
        .allocator = allocator,
    };
}
