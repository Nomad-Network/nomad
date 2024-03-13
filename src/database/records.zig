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
    const owner_hash = owner orelse std.mem.zeroes([64]u8);
    const record_perms = permissions orelse 0;
    return Self{ .owner = owner_hash, .data = data, .permissions = record_perms, .last_ping = 0, .allocator = allocator };
}

pub fn default(allocator: std.mem.Allocator) Self {
    return Self{
        .allocator = allocator,
        .owner = std.mem.zeroes([64]u8),
        .data = std.mem.zeroes([2048]u8),
        .permissions = 0,
        .last_ping = 0,
    };
}

pub fn getTypeSize() usize {
    return 64 + 2048 + 1 + 16;
}

pub fn hash(self: Self) !u64 {
    const hash64 = std.hash.CityHash64.hash(try self.serialize());
    return hash64;
}

pub fn deserialize(self: Self, content: []u8) !Self {
    if (getTypeSize() != content.len) return error.RecordInvalid;

    const owner = content[0..64];
    const data = content[64 .. 64 + 2048];
    const permissions = content[64 + 2048];
    const last_ping: i128 = @bitCast(mem_utils.sixteenBytesToU128(content[64 + 2048 + 1 .. 64 + 2048 + 16 + 1]));

    return Self{ .owner = owner.*, .data = data.*, .permissions = permissions, .last_ping = last_ping, .allocator = self.allocator };
}

pub fn serialize(self: Self) ![]u8 {
    var container = std.ArrayList(u8).init(self.allocator);
    try container.appendSlice(&self.owner);
    try container.appendSlice(&self.data);
    try container.append(self.permissions);
    try container.appendSlice(&mem_utils.u128ToSixteenBytes(@bitCast(self.last_ping), .little));

    return container.items;
}
