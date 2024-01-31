const std = @import("std");
const mem_utils = @import("../utils/mem.zig");

const Self = @This();

const RecordsErr = error{RecordInvalid};

owner: [64]u8,
data: [2048]u8,
permissions: u8,
last_ping: i128,
allocator: std.mem.Allocator,

pub fn init(allocator: std.mem.Allocator, data: [2048]u8, owner: ?[64]u8, permissions: ?u8) Self {
    return Self{ .owner = owner orelse [_]u8{0} ** 64, .data = data, .permissions = permissions orelse 0, .last_ping = 0, .allocator = allocator };
}

pub fn getTypeSize() u64 {
    return @sizeOf(Self);
}

pub fn deserialize(self: Self, content: []u8) !Self {
    if (self.getTypeSize() != content.len) return error.RecordInvalid;

    const last_ping = @as(i128, mem_utils.sixteenBytesToU128(content[64 + 2048 + 1 ..]));

    return Self{
        .owner = content[0..64],
        .data = content[64 .. 64 + 2048],
        .permissions = content[64 + 2048],
        .last_ping = last_ping,
    };
}

pub fn serialize(self: Self) ![]u8 {
    const container = std.ArrayList(u8).init(self.allocator);
    try container.appendSlice(self.owner);
    try container.appendSlice(self.data);
    try container.append(self.permissions);
    try container.appendSlice(mem_utils.u128ToSixteenBytes(@as(u128, self.last_ping)));

    return container.items;
}
