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

package com.dangdang.ddframe.job.cloud.api;

import com.dangdang.ddframe.job.cloud.executor.TaskExecutor;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.springframework.context.ApplicationContext;

/**
 * 云作业启动器.
 *
 * <p>需将应用打包, 并在main方法中直接调用Bootstrap.execute方法</p>
 *
 * @author caohao
 * @author zhangliang
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JobBootstrap {

    /**
     * 执行作业.
     */
    public static void execute() {
        MesosExecutorDriver driver = new MesosExecutorDriver(new TaskExecutor());
        System.exit(Protos.Status.DRIVER_STOPPED == driver.run() ? 0 : -1);
    }

    /**
     * 执行作业.
     * 支持Springboot框架下的job
     */
    public static void execute(ApplicationContext ctx) {
        MesosExecutorDriver driver = new MesosExecutorDriver(new TaskExecutor(ctx));
        System.exit(Protos.Status.DRIVER_STOPPED == driver.run() ? 0 : -1);
    }
}
