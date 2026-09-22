package com.github.h3yow.reliablemessagequeuejava.queue.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.reliablemessagequeuejava.queue.state.BrokerState;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * 基于 JSON 快照文件的本地持久化存储。
 *
 * <p>每次状态变更都将完整状态序列化到临时文件，再原子替换目标文件，
 * 保证应用重启后读到的要么是旧快照、要么是新快照，不会出现半截文件。
 */
public class StateStore {

    private final Path dataFile;
    private final ObjectMapper mapper;

    public StateStore(Path dataFile) {
        this.dataFile = dataFile;
        this.mapper = new ObjectMapper();
    }

    public BrokerState load() {
        if (!Files.exists(dataFile)) {
            return new BrokerState();
        }
        try {
            if (Files.size(dataFile) == 0) {
                return new BrokerState();
            }
            BrokerState state = mapper.readValue(dataFile.toFile(), BrokerState.class);
            return state == null ? new BrokerState() : state;
        } catch (IOException e) {
            throw new UncheckedIOException("加载队列状态失败: " + dataFile, e);
        }
    }

    public void save(BrokerState state) {
        try {
            Path parent = dataFile.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = Files.createTempFile(parent, "queue-state", ".tmp");
            try {
                mapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), state);
                try {
                    Files.move(tmp, dataFile, StandardCopyOption.ATOMIC_MOVE,
                            StandardCopyOption.REPLACE_EXISTING);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(tmp, dataFile, StandardCopyOption.REPLACE_EXISTING);
                }
            } finally {
                Files.deleteIfExists(tmp);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("持久化队列状态失败: " + dataFile, e);
        }
    }
}
