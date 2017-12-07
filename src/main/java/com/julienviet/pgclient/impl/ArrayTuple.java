/*
 * Copyright (C) 2017 Julien Viet
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.julienviet.pgclient.impl;

import com.julienviet.pgclient.Tuple;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.time.LocalDateTime;
import java.time.temporal.Temporal;
import java.util.ArrayList;

public class ArrayTuple extends ArrayList<Object> implements Tuple {

  public static Tuple EMPTY = new ArrayTuple(0);

  @Override
  public Boolean getBoolean(int pos) {
    return (Boolean) get(pos);
  }

  @Override
  public Object getValue(int pos) {
    return get(pos);
  }

  @Override
  public Integer getInteger(int pos) {
    return (Integer) get(pos);
  }

  @Override
  public Long getLong(int pos) {
    return (Long) get(pos);
  }

  @Override
  public Float getFloat(int pos) {
    return (Float) get(pos);
  }

  @Override
  public Double getDouble(int pos) {
    return (Double) get(pos);
  }

  @Override
  public String getString(int pos) {
    return (String) get(pos);
  }

  @Override
  public JsonObject getJsonObject(int pos) {
    return (JsonObject) get(pos);
  }

  @Override
  public JsonArray getJsonArray(int pos) {
    return (JsonArray) get(pos);
  }

  @Override
  public Buffer getBinary(int pos) {
    return (Buffer) get(pos);
  }

  @Override
  public Temporal getTemporal(int pos) {
    return (Temporal) get(pos);
  }

  @Override
  public Tuple addBoolean(Boolean value) {
    return this;
  }

  @Override
  public Tuple addValue(Object value) {
    add(value);
    return this;
  }

  @Override
  public Tuple addInteger(Integer value) {
    add(value);
    return this;
  }

  @Override
  public Tuple addLong(Long value) {
    add(value);
    return this;
  }

  @Override
  public Tuple addFloat(Float value) {
    add(value);
    return this;
  }

  @Override
  public Tuple addDouble(Double value) {
    add(value);
    return this;
  }

  @Override
  public Tuple addString(String value) {
    add(value);
    return this;
  }

  @Override
  public Tuple addJsonObject(JsonObject value) {
    add(value);
    return this;
  }

  @Override
  public Tuple addJsonArray(JsonArray value) {
    add(value);
    return this;
  }

  @Override
  public Tuple addBinary(Buffer value) {
    add(value);
    return this;
  }

  @Override
  public Tuple getTemporal(Temporal value) {
    add(value);
    return this;
  }

  public ArrayTuple(int len) {
    super(len);
  }


}