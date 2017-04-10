/*
 * Copyright 2013-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.sleuth.instrument.async.issues.issue546;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.sleuth.Sampler;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.cloud.sleuth.sampler.AlwaysSampler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.AsyncRestTemplate;
import org.springframework.web.client.RestTemplate;

import com.jayway.awaitility.Awaitility;

import static org.springframework.cloud.sleuth.assertions.SleuthAssertions.then;

/**
 * @author Marcin Grzejszczak
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class,
		properties = {"ribbon.eureka.enabled=false", "feign.hystrix.enabled=false", "server.port=0"},
		webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class Issue546Tests {

	@Autowired Environment environment;
	@Autowired Tracer tracer;
	@Autowired AsyncTask asyncTask;
	@Autowired RestTemplate restTemplate;
	/**
	 * Related to issue #445
	 */
	@Autowired Application.MyService executorService;

	@Test
	public void should_pass_tracing_info_for_tasks_running_without_a_pool() {
		Span span = this.tracer.createSpan("foo");
		try {
			String response = this.restTemplate.getForObject("http://localhost:" + port() + "/without_pool", String.class);

			then(response).isEqualTo(span.traceIdString());
			Awaitility.await().until(() -> {
				then(this.asyncTask.getSpan().get()).isNotNull();
				then(this.asyncTask.getSpan().get().getTraceId()).isEqualTo(span.getTraceId());
			});
		} finally {
			this.tracer.close(span);
		}
	}

	private int port() {
		return this.environment.getProperty("local.server.port", Integer.class);
	}
}

@Configuration
@EnableAsync
class AppConfig {

	@Bean public Sampler testSampler() {
		return new AlwaysSampler();
	}

	@Bean public RestTemplate restTemplate() {
		return new RestTemplate();
	}

	@Bean public Executor poolTaskExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.initialize();
		return executor;
	}

}

@RestController
class Controller {
	private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

	private final AsyncRestTemplate traceAsyncRestTemplate;

	private final Task task;

	public Controller(AsyncRestTemplate traceAsyncRestTemplate, Task task) {
		this.traceAsyncRestTemplate = traceAsyncRestTemplate;
		this.task = task;
	}

	@Value("${server.port}") private String port;

	@Value("${app.activatesSleep:true}") private boolean activatesSleep;

	@RequestMapping(value = "/bean") public HogeBean bean() {
		log.info("(/bean) I got a request!");
		return new HogeBean("test", 18);
	}

	@RequestMapping(value = "/trace-async-rest-template")
	public void asyncTest(@RequestParam(required = false) boolean isSleep)
			throws InterruptedException {
		log.info("(/trace-async-rest-template) I got a request!");
		ListenableFuture<ResponseEntity<HogeBean>> res = traceAsyncRestTemplate
				.getForEntity("http://localhost:" + port + "/bean", HogeBean.class);
		if (isSleep) {
			Thread.sleep(1000);
		}
		res.addCallback(success -> {
			log.info("(/trace-async-rest-template) success");
		}, failure -> {
			log.error("(/trace-async-rest-template) failure", failure);
		});
	}

	@RequestMapping(value = "/async-annotation") public void asyncTest2()
			throws InterruptedException {
		log.info("(/async-annotation) start");
		task.doSomething();
		log.info("(/async-annotation) end");
	}
}

@Component
class Task {

	private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

	@Async
	void doSomething() {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			log.error(e.getMessage(), e);
		}

		log.info("doSomething() done");
	}
}

class HogeBean {
	private String name;
	private int age;

	public HogeBean(String name, int age) {
		this.name = name;
		this.age = age;
	}

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return this.age;
	}

	public void setAge(int age) {
		this.age = age;
	}
}