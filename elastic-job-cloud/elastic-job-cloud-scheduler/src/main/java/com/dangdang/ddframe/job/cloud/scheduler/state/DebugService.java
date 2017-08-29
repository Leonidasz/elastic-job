/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.job.cloud.scheduler.state;

import com.dangdang.ddframe.job.context.TaskContext;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.mesos.Protos;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 错误定位服务.
 * 
 * @author gaohongtao
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class DebugService {
    
    public static final DebugService INSTANCE = new DebugService();
    
    private final LoadingCache<String, Map<Integer, Snapshot>> state = CacheBuilder.newBuilder().expireAfterAccess(30, TimeUnit.MINUTES).build(new CacheLoader<String, Map<Integer, Snapshot>>() {
        @Override
        public Map<Integer, Snapshot> load(final String key) throws Exception {
            return new HashMap<>();
        }
    });
    
    private final Lock lock = new ReentrantLock();
    
    /**
     * 获得当前任务快照.
     * 
     * @param jobName 作业名称
     * @return 作业所有分片的快照
     */
    public Map<Integer, Snapshot> capture(final String jobName) {
        return state.getIfPresent(jobName);
    }
    
    
    /**
     * 记录任务轨迹.
     * 
     * @param taskContext 任务上下文
     * @param taskStatus 任务状态
     */
    public void recordTask(final TaskContext taskContext, final Protos.TaskStatus taskStatus) {
        lock.lock();
        try {
            Map<Integer, Snapshot> snapshotMap = state.getUnchecked(taskContext.getMetaInfo().getJobName());
            Snapshot snapshot = snapshotMap.get(taskContext.getMetaInfo().getShardingItems().get(0));
            if (null == snapshot) {
                snapshot = new Snapshot();
                snapshotMap.put(taskContext.getMetaInfo().getShardingItems().get(0), snapshot);
            }
            if (taskStatus.getState().name().endsWith("ING")) {
                snapshot.unTerminalState = taskStatus.getState();
            } else {
                snapshot.terminalState = taskStatus.getState();
                snapshot.terminalMessage = taskStatus.getMessage();
            }
            snapshot.taskId = taskContext.getId();
            snapshot.latestState = taskStatus.getState();
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * 任务快照对象.
     * 任务快照区分了终态状态与非终态状态
     */
    @Getter
    public static class Snapshot {
        
        private String taskId;
        
        private Protos.TaskState terminalState;
        
        private Protos.TaskState unTerminalState;
        
        private Protos.TaskState latestState;
        
        private String terminalMessage;
        
    }
}
