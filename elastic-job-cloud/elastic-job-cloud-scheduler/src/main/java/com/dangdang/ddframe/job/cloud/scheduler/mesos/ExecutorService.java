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

package com.dangdang.ddframe.job.cloud.scheduler.mesos;

import com.dangdang.ddframe.job.cloud.scheduler.producer.ProducerManager;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.SlaveID;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 运行期清理服务.
 * 
 * @author gaohongtao
 */
@Slf4j
@RequiredArgsConstructor
public class ExecutorService {
    
    private final ListeningExecutorService es = MoreExecutors.listeningDecorator(MoreExecutors
            .getExitingExecutorService((ThreadPoolExecutor) Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Clean-Executor-%d").build())));
    
    private final ProducerManager producerManager;
    
    private final MesosStateService mesosStateService;
    
    /**
     * 清理运行器.
     * 
     * @param appName 应用名称
     */
    public void clean(final String appName) {
        execute(new Cleaner(appName));
    }
    
    /**
     * 停止应用.
     * 
     * @param appName 应用名称
     * @return true 停止命令发送成功 false 可以停止的应用
     */
    public boolean stop(final String appName) {
        Collection<MesosStateService.ExecutorStateInfo> executorBriefInfo = mesosStateService.executors(appName);
        if (executorBriefInfo.isEmpty()) {
            return false;
        }
        for (MesosStateService.ExecutorStateInfo each : executorBriefInfo) {
            producerManager.sendFrameworkMessage(ExecutorID.newBuilder().setValue(each.getId()).build(),
                    SlaveID.newBuilder().setValue(each.getSlaveId()).build(), "STOP".getBytes());
        }
        return true;
    }
    
    
    private void execute(final Cleaner cleaner) {
        final ListenableFuture<Boolean> future = es.submit(cleaner);
        future.addListener(new Runnable() {
            @Override
            public void run() {
                try {
                    if (future.get()) {
                        reSubmit();
                    }
                } catch (final ExecutionException e) {
                    log.error("Executor Clean error", e);
                    reSubmit();
                } catch (final InterruptedException ignored) {
                }
            }
            
            private void reSubmit() {
                try {
                    Thread.sleep(1000 * 5);
                } catch (final InterruptedException ignored) {
                    return;
                }
                execute(cleaner);
            }
        }, es);
    }
    
    @RequiredArgsConstructor
    private class Cleaner implements Callable<Boolean> {
        
        private final String appName;
        
        private volatile int sendTimes;
        
        @Override
        public Boolean call() throws Exception {
            return ++sendTimes > 5 || stop(appName);
        }
    }
}
