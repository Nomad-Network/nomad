const std = @import("std");

const utils = @import("../utils/file.zig");
const LookupTable = @import("./lookuptable.zig");
const Header = @import("./header.zig");
const Record = @import("./records.zig");
const iter_utils = @import("../utils/iter.zig");

const Self = @This();

const DBError = error{RecordDeletionError};

const Context = struct {
    database_handle: ?*Self,
};

gpa: std.heap.GeneralPurposeAllocator(.{}),
context: Context,
header: Header,
lookup_table: LookupTable,
records: std.ArrayList(Record),
deleted_records: std.ArrayList(Record),
path: []const u8,
content: []const u8,

pub fn init(file: []const u8) !Self {
    const exists = utils.fileExists(file, .{ .create_if_not_exist = true });
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    
    if (!exists) {
        try utils.writeFile(file, try Header.default(gpa.allocator()).serialize());
    }

    const content = try utils.readFile(gpa.allocator(), file);
    const header = Header.init(gpa.allocator(), content);
    var lookup_table = LookupTable.init(gpa.allocator());
    lookup_table = try lookup_table.deserialize(content, header);

    const records = std.ArrayList(Record).init(gpa.allocator());
    const deleted_records = std.ArrayList(Record).init(gpa.allocator());

    var self = Self{ .gpa = gpa, .path = file, .content = content, .header = header, .records = records, .lookup_table = lookup_table, .deleted_records = deleted_records, .context = Context{ .database_handle = null, }, };
    self.context.database_handle = &self;

    return deserialize(&self, @constCast(content));
}

pub fn addRecord(self: *Self, record: Record) !u64 {
    const hash = try record.hash();

    if (self.lookup_table.hasRecord(hash)) {
        return hash;
    }

    var pos = self.records.items.len;
    const deleted_record_count = self.lookup_table.deleted_table.count();

    if (deleted_record_count > 0) {
        self.records.items[deleted_record_count].deinit();
        self.records.items[deleted_record_count] = record;

        var iterator = self.lookup_table.deleted_table.keyIterator();
        const first_hash = iterator.next().?;
        pos = self.lookup_table.deleted_table.get(first_hash.*).?;

        const ok = self.lookup_table.deleted_table.remove(first_hash.*);

        if (!ok) return DBError.RecordDeletionError;
    }

    try self.lookup_table.addRecord(hash, pos);
    try self.records.append(record);

    return hash;
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

    std.debug.print("{s}", .{string.items});
}

pub fn getRecord(self: *Self, hash: u64) !Record {
    const record_pos = self.lookup_table.getRecord(hash);

    if (record_pos) |pos| {
        return self.records.items[pos];
    }

    return try Record.init(null, null);
}

pub fn deleteRecord(self: *Self, hash: u64) !bool {
    const record_pos = self.lookup_table.getRecord(hash);

    if (record_pos) |pos| {
        try self.lookup_table.deleteRecord(hash, pos);
        return true;
    }

    return false;
}

fn deserialize(self: *Self, content: []u8) !Self {
    var records_iter = iter_utils.SteppedIterator(u8, Record.getTypeSize()){ .items = content[self.header.records_offset..self.header.deleted_offset] };
    var deleted_iter = iter_utils.SteppedIterator(u8, Record.getTypeSize()){ .items = content[self.header.deleted_offset..] };

    while (records_iter.next()) |bytes| {
        var record = try Record.default();
        try record.deserialize(bytes);
        try self.records.append(record);
    }

    while (deleted_iter.next()) |bytes| {
        var record = try Record.default();
        try record.deserialize(bytes);
        try self.deleted_records.append(record);
    }

    return self.*;
}

pub fn serialize(self: *Self) ![]u8 {
    const records_len = self.records.items.len;
    const deleted_records_len = self.deleted_records.items.len;

    const records_size: u64 = records_len * Record.getTypeSize();
    const deleted_records_size: u64 = deleted_records_len * Record.getTypeSize();

    const serialized_table = try self.lookup_table.serialize();

    self.header.is_valid = true;

    self.header.lookup_len = serialized_table.len;
    self.header.records_len = records_len;
    self.header.deleted_len = deleted_records_len;

    self.header.records_offset = self.header.lookup_offset + serialized_table.len;
    self.header.deleted_offset = self.header.lookup_offset + serialized_table.len + records_size;

    const header = try self.header.serialize();

    var buffer_list = try self.allocator.alloc(u8, header.len + serialized_table.len + records_size + deleted_records_size);

    std.mem.copyForwards(u8, buffer_list[0..header.len], header);
    std.mem.copyForwards(u8, buffer_list[header.len .. header.len + serialized_table.len], serialized_table);

    for (self.records.items, 0..) |record, i| {
        var mut_record = record;
        std.mem.copyForwards(
            u8,
            buffer_list[header.len + serialized_table.len + (Record.getTypeSize() * i) .. header.len + serialized_table.len + (Record.getTypeSize() * (i + 1))],
            try mut_record.serialize(),
        );
    }

    for (self.deleted_records.items, 0..) |deleted_record, i| {
        var mut_record = deleted_record;
        std.log.info("DEL: {any} {any}", .{ deleted_record, i });
        std.mem.copyForwards(
            u8,
            buffer_list[header.len + serialized_table.len + records_size + (Record.getTypeSize() * i) .. header.len + serialized_table.len + records_size + (Record.getTypeSize() * (i + 1))],
            try mut_record.serialize(),
        );
    }

    return buffer_list;
}

pub fn commit(self: *Self) !void {
    const content = try self.serialize();
    try utils.writeFile(self.path, content);
}
