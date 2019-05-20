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

package com.dangdang.ddframe.job.lite.internal.schedule;

import com.dangdang.ddframe.job.lite.internal.sharding.ExecutionService;
import com.dangdang.ddframe.job.lite.internal.sharding.ShardingService;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobExecutionContext;
import org.quartz.Trigger;
import org.quartz.listeners.TriggerListenerSupport;

/**
 * 作业触发监听器.
 * 
 * @author zhangliang
 */
@RequiredArgsConstructor
@Slf4j
public final class JobTriggerListener extends TriggerListenerSupport {

    //trigger listener metrics
    //esjob最近一次fire时间
    private static final Gauge esjob_fire_time_mseconds = Gauge.build().name("esjob_fire_time_mseconds")
            .help("The last fire time of job in mseconds.")
            .labelNames("job_name","sharding_items")
            .register();

    //esjob最近一次scheduledfire时间
    private static final Gauge esjob_scheduledfire_time_mseconds = Gauge.build().name("esjob_scheduledfire_time_mseconds")
            .help("The last scheduledfire time of job in mseconds.")
            .labelNames("job_name","sharding_items")
            .register();

    //esjob上一次fire时间
    private static final Gauge esjob_prefire_time_mseconds = Gauge.build().name("esjob_prefire_time_mseconds")
            .help("The previous fire time of job in mseconds.")
            .labelNames("job_name","sharding_items")
            .register();

    //esjob下一次fire时间
    //如果当前时间已经超过了esjob下一次的fire时间，说明作业有misfire产生
    private static final Gauge esjob_nextfire_time_mseconds = Gauge.build().name("esjob_nextfire_time_mseconds")
            .help("The next fire time of job in mseconds.")
            .labelNames("job_name","sharding_items")
            .register();

    //misfire metrics
    //esjob最近一次misfire trigger触发时间
    private static final Gauge esjob_misfire_trigger_time_seconds = Gauge.build().name("esjob_misfire_trigger_time_seconds")
            .help("The last trigger time of job misfire in seconds.")
            .labelNames("job_name")
            .register();

    //esjob misfire trigger触发总次数
    private static final Counter esjob_misfire_trigger_total = Counter.build().name("esjob_misfire_trigger_total")
            .help("The total num of job misfired triggered.")
            .labelNames("job_name","sharding_items")
            .register();

    private final ExecutionService executionService;
    
    private final ShardingService shardingService;
    
    @Override
    public String getName() {
        return "JobTriggerListener";
    }

    @Override
    public void triggerFired(Trigger trigger, JobExecutionContext context) {
        esjob_fire_time_mseconds.labels(trigger.getJobKey().getName(),shardingService.getLocalShardingItems().toString()).set(context.getFireTime().getTime());
        esjob_scheduledfire_time_mseconds.labels(trigger.getJobKey().getName(),shardingService.getLocalShardingItems().toString()).set(context.getScheduledFireTime().getTime());
        esjob_prefire_time_mseconds.labels(trigger.getJobKey().getName(),shardingService.getLocalShardingItems().toString()).set(context.getPreviousFireTime() == null ? context.getScheduledFireTime().getTime():context.getPreviousFireTime().getTime());
        esjob_nextfire_time_mseconds.labels(trigger.getJobKey().getName(),shardingService.getLocalShardingItems().toString()).set(context.getNextFireTime().getTime());
    }


    @Override
    public void triggerMisfired(final Trigger trigger) {
        esjob_misfire_trigger_total.labels(trigger.getJobKey().getName()).inc();
        esjob_misfire_trigger_time_seconds.labels(trigger.getJobKey().getName()).setToCurrentTime();
        if (null != trigger.getPreviousFireTime()) {
            executionService.setMisfire(shardingService.getLocalShardingItems());
        }
    }

}
