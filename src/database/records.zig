const std = @import("std");
const mem_utils = @import("../utils/mem.zig");

const Self = @This();

const RecordsErr = error{ RecordInvalid, RecordDataLengthTooLong };

owner: [64]u8,
data: []u8,
permissions: u8,
last_ping: i128,
gpa: std.heap.GeneralPurposeAllocator(.{}),

pub fn init(owner: ?[64]u8, permissions: ?u8) !Self {
    const owner_hash = owner orelse std.mem.zeroes([64]u8);
    const record_perms = permissions orelse 0;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    return Self{
        .owner = owner_hash,
        .data = try gpa.allocator().alloc(u8, 2048),
        .permissions = record_perms,
        .last_ping = 0,
        .gpa = gpa,
    };
}

pub fn default() !Self {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    return Self{
        .owner = std.mem.zeroes([64]u8),
        .data = try gpa.allocator().alloc(u8, 2048),
        .permissions = 0,
        .last_ping = 0,
        .gpa = gpa,
    };
}

pub fn getTypeSize() usize {
    return 64 + 2048 + 1 + 16;
}

fn allocator(self: *Self) std.mem.Allocator {
    return self.gpa.allocator();
}

pub fn setData(self: *Self, data: []u8) !void {
    if (data.len > 2048) return RecordsErr.RecordDataLengthTooLong;

    const zeroes = try self.allocator().alloc(u8, 2048);
    std.mem.copyForwards(u8, self.data, zeroes);
    std.mem.copyForwards(u8, self.data, data);
}

pub fn hash(self: Self) !u64 {
    var mut_self = self;
    const hash64 = std.hash.CityHash64.hash(try mut_self.serialize());
    return hash64;
}

pub fn getData(self: Self) []u8 {
    return self.data[0..2048];
}

pub fn deserialize(self: *Self, content: []u8) !void {
    if (getTypeSize() != content.len) return error.RecordInvalid;

    var container = try self.allocator().alloc(u8, Self.getTypeSize());
    std.mem.copyForwards(u8, container, content);

    std.mem.copyForwards(u8, &self.owner, container[0..64]);
    std.mem.copyForwards(u8, self.data, container[64 .. 64 + 2048]);
    self.permissions = container[64 + 2048];
    self.last_ping = @bitCast(mem_utils.sixteenBytesToU128(container[64 + 2048 + 1 .. 64 + 2048 + 16 + 1]));
}

pub fn serialize(self: *Self) ![]u8 {
    var container = try self.allocator().alloc(u8, Self.getTypeSize());
    std.mem.copyForwards(u8, container[0..64], &self.owner);
    std.mem.copyForwards(u8, container[64 .. 64 + 2048], self.getData());
    container[64 + 2048] = self.permissions;
    std.mem.copyForwards(u8, container[64 + 2048 + 1 .. 64 + 2048 + 16 + 1], &mem_utils.u128ToSixteenBytes(@bitCast(self.last_ping), .little));

    std.log.debug("RSER: {any}", .{container});
    return container;
}

pub fn deinit(self: *Self) void {
    _ = self.gpa.deinit();
}
