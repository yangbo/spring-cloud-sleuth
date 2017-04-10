/*
 * Copyright 2013-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.springframework.cloud.sleuth.instrument.async;

import java.lang.reflect.Method;
import java.util.concurrent.Executor;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.TraceKeys;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.cloud.sleuth.util.SpanNameUtil;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.ReflectionUtils;

/**
 * Aspect that creates a new Span for running threads executing methods annotated with
 * {@link org.springframework.scheduling.annotation.Async} annotation.
 *
 * @author Marcin Grzejszczak
 * @since 1.0.0
 *
 * @see Tracer
 */
@Aspect
public class TraceAsyncAspect {

	private static final String ASYNC_COMPONENT = "async";

	private final Tracer tracer;
	private final TraceKeys traceKeys;
	private final BeanFactory beanFactory;

	public TraceAsyncAspect(Tracer tracer, TraceKeys traceKeys, BeanFactory beanFactory) {
		this.tracer = tracer;
		this.traceKeys = traceKeys;
		this.beanFactory = beanFactory;
	}

	@Around("execution (@org.springframework.scheduling.annotation.Async  * *.*(..)) && !within(is(FinalType))")
	public Object traceBackgroundThread(final ProceedingJoinPoint pjp) throws Throwable {
		Span span = this.tracer.createSpan(
				SpanNameUtil.toLowerHyphen(pjp.getSignature().getName()));
		this.tracer.addTag(Span.SPAN_LOCAL_COMPONENT_TAG_NAME, ASYNC_COMPONENT);
		this.tracer.addTag(this.traceKeys.getAsync().getPrefix() +
				this.traceKeys.getAsync().getClassNameKey(), pjp.getTarget().getClass().getSimpleName());
		this.tracer.addTag(this.traceKeys.getAsync().getPrefix() +
				this.traceKeys.getAsync().getMethodNameKey(), pjp.getSignature().getName());
		try {
			return pjp.proceed();
		} finally {
			this.tracer.close(span);
		}
	}

	@Around("execution (* org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor.*(..)) && !within(is(FinalType))")
	public Object traceThreadPoolTaskExecutor(final ProceedingJoinPoint pjp) throws Throwable {
		LazyTraceThreadPoolTaskExecutor executor = new LazyTraceThreadPoolTaskExecutor(this.beanFactory,
				(ThreadPoolTaskExecutor) pjp.getTarget());
		Method methodOnTracedBean = getMethod(pjp, executor);
		if (methodOnTracedBean != null) {
			return methodOnTracedBean.invoke(executor, pjp.getArgs());
		}
		return pjp.proceed();
	}

	@Around("execution (* java.util.concurrent.Executor.*(..)) && !within(is(FinalType))")
	public Object traceExecutor(final ProceedingJoinPoint pjp) throws Throwable {
		LazyTraceExecutor executor = new LazyTraceExecutor(this.beanFactory,
				(Executor) pjp.getTarget());
		Method methodOnTracedBean = getMethod(pjp, executor);
		if (methodOnTracedBean != null) {
			return methodOnTracedBean.invoke(executor, pjp.getArgs());
		}
		return pjp.proceed();
	}

	private Method getMethod(ProceedingJoinPoint pjp, Object object) {
		MethodSignature signature = (MethodSignature) pjp.getSignature();
		Method method = signature.getMethod();
		return ReflectionUtils
				.findMethod(object.getClass(), method.getName(), method.getParameterTypes());
	}

}
