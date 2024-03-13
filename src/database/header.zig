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
    const is_valid = content.len >= 28 and std.mem.eql(u8, "xND", content[0..3]) and content[3] == 0; // xND\0

    if (!is_valid) {
        var header = Self.default(allocator);
        header.is_valid = false;
        return header;
    }

    const lookup_offset = utils.eightBytesToU64(content[5..13]);
    const records_offset = utils.eightBytesToU64(content[13..21]);
    const deleted_offset = utils.eightBytesToU64(content[21..29]);

    return Self{
        .is_valid = is_valid,
        .lookup_offset = lookup_offset,
        .records_offset = records_offset,
        .deleted_offset = deleted_offset,

        .lookup_len = records_offset - lookup_offset,
        .records_len = deleted_offset - records_offset,
        .deleted_len = content.len - deleted_offset,

        .allocator = allocator,
    };
}

pub fn default(allocator: std.mem.Allocator) Self {
    return Self{
        .is_valid = true,

        // For a new nomad data file, the offsets are all set to just after the header
        .lookup_offset = Self.getSize(),
        .records_offset = Self.getSize(),
        .deleted_offset = Self.getSize(),

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

pub fn getSize() comptime_int {
    return @sizeOf(Self) - @sizeOf(std.mem.Allocator) - 3;
}
