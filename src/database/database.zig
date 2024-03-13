const std = @import("std");

const utils = @import("../utils/file.zig");
const LookupTable = @import("./lookuptable.zig");
const Header = @import("./header.zig");
const Record = @import("./records.zig");
const iter_utils = @import("../utils/iter.zig");

const Self = @This();

const Context = struct {
    database_handle: ?*Self,
};

context: Context,
header: Header,
lookup_table: LookupTable,
allocator: std.mem.Allocator,
records: std.ArrayList(*Record),
deleted_records: std.ArrayList(*Record),
path: []const u8,
content: []const u8,

pub fn init(allocator: std.mem.Allocator, file: []const u8) !Self {
    const exists = utils.fileExists(file, .{ .create_if_not_exist = true });

    if (!exists) {
        try utils.writeFile(file, try Header.default(allocator).serialize());
    }

    const content = try utils.readFile(allocator, file);
    const header = Header.init(allocator, content);
    var lookup_table = LookupTable.init(allocator);
    lookup_table = try lookup_table.deserialize(content, header);

    const records = std.ArrayList(*Record).init(allocator);
    const deleted_records = std.ArrayList(*Record).init(allocator);

    var self = Self{ .allocator = allocator, .path = file, .content = content, .header = header, .records = records, .lookup_table = lookup_table, .deleted_records = deleted_records, .context = Context{ .database_handle = null } };
    self.context.database_handle = &self;

    return deserialize(&self, @constCast(content));
}

pub fn addRecord(self: *Self, record: *Record) !void {
    const hash = try record.*.hash();

    std.log.debug("Record with hash 0x{X:0>8}, {any}", .{ hash, self.lookup_table.hasRecord(hash) });

    if (self.lookup_table.hasRecord(hash)) {
        return;
    }

    try self.lookup_table.addRecord(hash, self.records.items.len);
    try self.records.append(record);
}

pub fn print(self: *Self) !void {
    var string = std.ArrayList(u8).init(self.allocator);
    try string.append('\n');
    try string.appendSlice("RECORDS TABLE\n");

    var keys_iterator = self.lookup_table.records_table.keyIterator();

    while (keys_iterator.next()) |key| {
        const value = self.lookup_table.records_table.get(key.*) orelse 0;
        try string.appendSlice(try std.fmt.allocPrint(self.allocator, "0x{X:0>16}|\t0x{X:0>16}\n", .{ key.*, value }));
    }

    try string.append('\n');
    try string.appendSlice("DELETED RECORDS TABLE\n");

    keys_iterator = self.lookup_table.deleted_table.keyIterator();

    while (keys_iterator.next()) |key| {
        const value = self.lookup_table.deleted_table.get(key.*) orelse 0;
        try string.appendSlice(try std.fmt.allocPrint(self.allocator, "0x{X:0>16}|\t0x{X:0>16}\n", .{ key.*, value }));
    }
}

pub fn getRecord(self: *Self, hash: u64) !*Record {
    const record_pos = self.lookup_table.getRecord(hash);

    if (record_pos) |pos| {
        return self.records.items[pos];
    }

    var blank_record = Record.init(self.allocator, std.mem.zeroes([2048]u8), null, null);

    return &blank_record;
}

fn deserialize(self: *Self, content: []u8) !Self {
    var records_iter = iter_utils.SteppedIterator(u8, Record.getTypeSize()){ .items = content[self.header.records_offset..self.header.deleted_offset] };
    var deleted_iter = iter_utils.SteppedIterator(u8, Record.getTypeSize()){ .items = content[self.header.deleted_offset..] };

    while (records_iter.next()) |bytes| {
        var record = Record.default(self.allocator);

        record = try record.deserialize(bytes);

        try self.records.append(&record);
    }

    while (deleted_iter.next()) |bytes| {
        var record = Record.default(self.allocator);

        record = try record.deserialize(bytes);

        try self.deleted_records.append(&record);
    }

    return self.*;
}

pub fn serialize(self: *Self) ![]u8 {
    var buffer_list = std.ArrayList(u8).init(self.allocator);

    const records_len = self.records.items.len;
    const deleted_records_len = self.deleted_records.items.len;

    const records_size: u64 = records_len * Record.getTypeSize();
    const serialized_table = try self.lookup_table.serialize();

    self.header.is_valid = true;

    self.header.lookup_len = serialized_table.len;
    self.header.records_len = records_len;
    self.header.deleted_len = deleted_records_len;

    self.header.records_offset = self.header.lookup_offset + serialized_table.len;
    self.header.deleted_offset = self.header.lookup_offset + serialized_table.len + records_size;

    try buffer_list.appendSlice(try self.header.serialize());
    try buffer_list.appendSlice(serialized_table);

    for (self.records.items) |record| {
        try buffer_list.appendSlice(try record.serialize());
    }

    for (self.deleted_records.items) |deleted_record| {
        try buffer_list.appendSlice(try deleted_record.serialize());
    }

    return buffer_list.items;
}

pub fn commit(self: *Self) !void {
    const content = try self.serialize();
    try utils.writeFile(self.path, content);
}
