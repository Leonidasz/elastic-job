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

package com.dangdang.ddframe.job.event.tsdb;

import com.dangdang.ddframe.job.event.JobEventListener;
import com.dangdang.ddframe.job.event.type.JobExecutionEvent;
import com.dangdang.ddframe.job.event.type.JobStatusTraceEvent;

/**
 * 运行痕迹事件时序数据库(TSDB)监听器.
 *
 * @author leonidas
 */
public final class JobEventTsdbListener extends JobEventTsdbIdentity implements JobEventListener {
    
    private final JobEventTsdbStorage repository;
    
    public JobEventTsdbListener() {
        repository = new JobEventTsdbStorage();
    }
    
    @Override
    public void listen(final JobExecutionEvent executionEvent) {
        repository.addJobExecutionEvent(executionEvent);
    }
    
    @Override
    public void listen(final JobStatusTraceEvent statusTraceEvent) {
        repository.addJobStatusTraceEvent(statusTraceEvent);
    }
}
