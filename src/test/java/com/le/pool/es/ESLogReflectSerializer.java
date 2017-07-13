package com.le.pool.es;

import com.google.common.collect.Maps;
import com.le.client.es.ESSerializer;
import org.apache.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ESLogReflectSerializer implements ESSerializer {
    private static Map<String, Method> propertyMethod = Maps.newConcurrentMap();
    private static Set<String> errorProperties = new HashSet<>();
    private static ThreadLocal<DateFormat> threadLocal = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };
    private static final Logger logger = Logger.getLogger(ESLogReflectSerializer.class);

    public static Date parseDate(String dateStr) throws ParseException {
        return threadLocal.get().parse(dateStr);

    }

    public static String formatDate(Date date) {
        return threadLocal.get().format(date);
    }

    public XContentBuilder getContentBuilder(Object obj) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();

        Class clazz = obj.getClass();
        Field[] fields = obj.getClass().getDeclaredFields();//获得属性
        boolean flag = false;

        for (Field field : fields) {
            String fieldName = field.getName();

            try {
                if (errorProperties.contains(fieldName)) {
                    continue;
                }
                Method getMethod = propertyMethod.get(fieldName);
                if (getMethod == null) {
                    PropertyDescriptor pd = new PropertyDescriptor(fieldName, clazz);
                    getMethod = pd.getReadMethod();
                    propertyMethod.put(fieldName, getMethod);
                }

                String value = (String) getMethod.invoke(obj);
                if (value == null || value.length() == 0) {
                    continue;
                }
                builder.field(field.getName(), value);
                if (field.getName().toLowerCase().contains("time")) {
                    try {
                        Date logTime = parseDate(value);
                        flag = true;
                        builder.field("@timestamp", logTime);
                    } catch (Exception e) {
                        logger.error("parse time error:" + value);
                    }
                }
            } catch (Exception e) { //没有set/get方法的属性跳过；
                if (propertyMethod.get(fieldName) == null) {
                    errorProperties.add(fieldName);
                }
            }
        }

        if (!flag) {
            builder.field("@timestamp", new Date());
        }

        builder.endObject();
        return builder;
    }
}
