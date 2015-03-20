/*
 * Joinery -- Data frames for Java
 * Copyright (c) 2014, 2015 IBM Corp.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package joinery.impl;

import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.ConstructorSignature;
import org.aspectj.lang.reflect.FieldSignature;
import org.aspectj.lang.reflect.MethodSignature;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.codahale.metrics.annotation.Timed;

@Aspect
public class Metrics {
    private static final MetricRegistry registry = SharedMetricRegistries.getOrCreate("joinery");

    private static Annotation getAnnotation(final Signature signature,
                                final Class<? extends Annotation> annotationClass) {
        if (signature instanceof ConstructorSignature) {
            final Constructor<?> ctor = ConstructorSignature.class.cast(signature)
                                    .getConstructor();
            return ctor.getAnnotation(annotationClass);
        } else if (signature instanceof MethodSignature) {
            return MethodSignature.class.cast(signature)
                    .getMethod()
                    .getAnnotation(annotationClass);
        } else if (signature instanceof FieldSignature) {
            return FieldSignature.class.cast(signature)
                    .getField()
                    .getAnnotation(annotationClass);
        }

        throw new RuntimeException(
                "Unsupported signature type " + signature.getClass().getName()
            );
    }

    private static String name(final Signature signature, final String name,
                        final String suffix, final boolean absolute) {
        String sig = signature.toString();

        // trim return type
        final int index = sig.indexOf(" ");
        if (index > 0) {
            sig = sig.substring(index + 1);
        }

        if (name.isEmpty()) {
            return MetricRegistry.name(sig, suffix);
        }

        if (!absolute) {
            return MetricRegistry.name(sig, name);
        }

        return name;
    }

    @Around("execution(@com.codahale.metrics.annotation.Timed * *(..))")
    public Object injectTimer(final ProceedingJoinPoint point)
    throws Throwable {
        final Signature signature = point.getSignature();
        final Annotation annotation = getAnnotation(signature, Timed.class);
        final Timed timed = Timed.class.cast(annotation);
        final String name = name(signature, timed.name(), "timer", timed.absolute());
        final Timer timer = registry.timer(name);
        final Timer.Context context = timer.time();
        try {
            return point.proceed(point.getArgs());
        } finally {
            context.stop();
        }
    }

    public static void displayMetrics() {
        ConsoleReporter.forRegistry(registry)
                .build()
                .report();
        CsvReporter.forRegistry(registry)
                .build(new File("target/"))
                .report();
    }
}
