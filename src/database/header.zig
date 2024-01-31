const std = @import("std");
const utils = @import("../utils/mem.zig");

const Self = @This();

const HeaderError = error{HeaderInvalid};

is_valid: bool = false,

lookup_len: u64 = 0,
records_len: u64 = 0,
deleted_len: u64 = 0,

lookup_offset: u64 = 0,
records_offset: u64 = 0,
deleted_offset: u64 = 0,

allocator: std.mem.Allocator,

pub fn init(allocator: std.mem.Allocator, content: []const u8) Self {
    const is_valid = content.len >= 28 and std.mem.eql(u8, "xND", content[0..2]) and content[3] == 0; // xND\0

    if (!is_valid) return Self.default(allocator);

    const lookup_offset = utils.eightBytesToU64(content[4..12]);
    const records_offset = utils.eightBytesToU64(content[12..20]);
    const deleted_offset = utils.eightBytesToU64(content[20..28]);

    return Self{
        .is_valid = is_valid,
        .lookup_offset = lookup_offset,
        .records_offset = records_offset,
        .deleted_offset = deleted_offset,

        .lookup_len = lookup_offset - records_offset,
        .records_len = records_offset - deleted_offset,
        .deleted_len = deleted_offset - content.len,

        .allocator = allocator,
    };
}

pub fn default(allocator: std.mem.Allocator) Self {
    return Self{
        .is_valid = true,
        .lookup_offset = 28,
        .records_offset = 28,
        .deleted_offset = 28,

        .lookup_len = 0,
        .records_len = 0,
        .deleted_len = 0,

        .allocator = allocator,
    };
}

pub fn serialize(self: Self) ![]u8 {
    var serialized = std.ArrayList(u8).init(self.allocator);

    try serialized.appendSlice(&[_]u8{ 'x', 'N', 'D', 0 });

    try serialized.append(@as(u8, @intFromBool(self.is_valid)));

    try serialized.appendSlice(&utils.u64ToEightBytes(self.lookup_offset, .little));
    try serialized.appendSlice(&utils.u64ToEightBytes(self.records_offset, .little));
    try serialized.appendSlice(&utils.u64ToEightBytes(self.deleted_offset, .little));

    try serialized.appendSlice(&utils.u64ToEightBytes(self.lookup_len, .little));
    try serialized.appendSlice(&utils.u64ToEightBytes(self.records_len, .little));
    try serialized.appendSlice(&utils.u64ToEightBytes(self.deleted_len, .little));

    return serialized.items;
}
