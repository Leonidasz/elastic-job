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

import com.dangdang.ddframe.job.event.type.JobExecutionEvent;
import com.dangdang.ddframe.job.event.type.JobStatusTraceEvent;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * 运行痕迹事件数据库存储.
 *
 * @author leonidas
 */
@Slf4j
@NoArgsConstructor
final class JobEventTsdbStorage {

    //excute metrics
    //esjob最近一次执行的开始时间
    private static final Gauge esjob_start_time_mseconds = Gauge.build().name("esjob_start_time_mseconds")
            .help("The last start time of job in mseconds.")
            .labelNames("job_name","task_id","instance_ip","source","sharding_item")
            .register();

    //esjob最近一次执行的完成时间
    private static final Gauge esjob_complete_time_mseconds = Gauge.build().name("esjob_complete_time_mseconds")
            .help("The last complete time of job in mseconds.")
            .labelNames("job_name","task_id","instance_ip","source","sharding_item","is_success")
            .register();

    //esjob执行次数
    private static final Counter esjob_execute_total = Counter.build().name("esjob_execute_total")
            .help("The total num of excuted job.")
            .labelNames("job_name","task_id","instance_ip","source","sharding_item","is_success")
            .register();

    //status metrics
    //esjob作业状态
    private static final Gauge esjob_status_trace = Gauge.build().name("esjob_status_trace")
            .help("The status trace of job.")
            .labelNames("job_name","task_id","slave_id","source","execution_type","sharding_items","state")
            .register();


    void addJobExecutionEvent(final JobExecutionEvent jobExecutionEvent) {
        if (null == jobExecutionEvent.getCompleteTime()) {
            esjob_start_time_mseconds
                    .labels(jobExecutionEvent.getJobName(),
                            jobExecutionEvent.getTaskId(),
                            jobExecutionEvent.getIp(),
                            //jobExecutionEvent.getHostname(),
                            jobExecutionEvent.getSource().name(),
                            String.valueOf(jobExecutionEvent.getShardingItem()))
                    .set(jobExecutionEvent.getStartTime().getTime());
        }else{
            String[] label_sa = {
                    jobExecutionEvent.getJobName(),
                    jobExecutionEvent.getTaskId(),
                    jobExecutionEvent.getIp(),
                    //jobExecutionEvent.getHostname(),
                    jobExecutionEvent.getSource().name(),
                    String.valueOf(jobExecutionEvent.getShardingItem()),
                    jobExecutionEvent.isSuccess() ? "1":"0"
            };

            //最近一次执行完成时间
            esjob_complete_time_mseconds.labels(label_sa).set(jobExecutionEvent.getCompleteTime().getTime());

            //执行次数
            esjob_execute_total.labels(label_sa).inc();
        }
    }


    void addJobStatusTraceEvent(final JobStatusTraceEvent jobStatusTraceEvent) {
        String[] label_sa = {
                jobStatusTraceEvent.getJobName(),
                jobStatusTraceEvent.getTaskId(),
                jobStatusTraceEvent.getSlaveId(),
                jobStatusTraceEvent.getSource().name(),
                jobStatusTraceEvent.getExecutionType().name(),
                jobStatusTraceEvent.getShardingItems(),
                jobStatusTraceEvent.getState().name()
        };
        esjob_status_trace.labels(label_sa).set(jobStatusTraceEvent.getCreationTime().getTime());
    }

}
