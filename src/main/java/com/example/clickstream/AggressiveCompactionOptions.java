package com.example.clickstream;

import org.apache.flink.state.rocksdb.RocksDBOptionsFactory;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import java.util.Collection;

public class AggressiveCompactionOptions implements RocksDBOptionsFactory {
    @Override
    public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        return currentOptions
                .setMaxBackgroundCompactions(4) // More background threads
                .setMaxBackgroundFlushes(2)
                .setIncreaseParallelism(6); // Or manually set max_background_jobs
    }

    @Override
    public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        return currentOptions
                .setLevel0FileNumCompactionTrigger(2) // Aggressive trigger
                .setMaxWriteBufferNumber(4)
                .setMinWriteBufferNumberToMerge(1)
                .setWriteBufferSize(4 * 1024 * 1024); // Smaller memtables
    }
}
