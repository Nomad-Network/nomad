const std = @import("std");
const utils = @import("../utils/file.zig");
const mem_utils = @import("../utils/mem.zig");
const iter_utils = @import("../utils/iter.zig");

const DataHeader = @import("./header.zig");
const Record = @import("./records.zig");

const Self = @This();

const InternalType = std.AutoHashMap(u64, u64);

records_table: InternalType,
deleted_table: InternalType,
allocator: std.mem.Allocator,

pub fn init(allocator: std.mem.Allocator) Self {
    return Self{
        .records_table = InternalType.init(allocator),
        .deleted_table = InternalType.init(allocator),
        .allocator = allocator,
    };
}

pub fn getSize(self: Self) u64 {
    const records_table_size = self.records_table.keyIterator().len;
    const deleted_table_size = self.deleted_table.keyIterator().len;

    return (records_table_size * 16 * 2) + (deleted_table_size * 16 * 2);
}

pub fn addRecord(self: *Self, hash: u64, pos: u64) !void {
    return try self.records_table.put(hash, pos);
}

pub fn getRecord(self: *Self, hash: u64) ?u64 {
    return self.records_table.get(hash);
}

pub fn hasRecord(self: *Self, hash: u64) bool {
    return self.records_table.contains(hash);
}

pub fn deserialize(self: *Self, content: []const u8, header: DataHeader) !Self {
    const lookup_offset = header.lookup_offset;
    const records_offset = header.records_offset;

    if (!header.is_valid) return self.*;
    if (lookup_offset == records_offset) return self.*;

    const table_bytes = try self.allocator.alloc(u8, records_offset - lookup_offset);
    std.mem.copyForwards(u8, table_bytes, content[lookup_offset..records_offset]);

    var table_iter = iter_utils.SteppedIterator(u8, 16){ .items = table_bytes };

    while (table_iter.next()) |entry| {
        const hash_slice = entry[0..8];
        const idx_slice = entry[8..];

        const hash = mem_utils.sliceToU64(hash_slice);
        const idx = mem_utils.sliceToU64(idx_slice);

        try self.records_table.put(hash, idx);
    }

    return self.*;
}

pub fn serialize(self: Self) ![]u8 {
    var buffer_list = std.ArrayList(u8).init(self.allocator);

    var records_iter = self.records_table.keyIterator();
    var deleted_iter = self.deleted_table.keyIterator();

    while (records_iter.next()) |key| {
        try buffer_list.appendSlice(&mem_utils.u64ToEightBytes(key.*, .little));
        try buffer_list.appendSlice(&mem_utils.u64ToEightBytes(self.records_table.get(key.*) orelse unreachable, .little));
    }

    while (deleted_iter.next()) |key| {
        try buffer_list.appendSlice(&mem_utils.u64ToEightBytes(key.*, .little));
        try buffer_list.appendSlice(&mem_utils.u64ToEightBytes(self.deleted_table.get(key.*) orelse unreachable, .little));
    }

    return buffer_list.items;
}
