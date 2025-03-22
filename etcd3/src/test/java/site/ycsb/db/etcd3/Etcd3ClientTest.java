/**
 * Copyright (c) 2025 José Marcelo Marques da Cruz.
 */

package site.ycsb.db.etcd3;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 *
 */
public class Etcd3ClientTest {
  private static final Integer NUMBER_OF_RECORDS = 20;
  private static final String PREFIX_KEY = "site.ycsb.db.etcd3.testkey";
  private static final String PREFIX_FIELD = "testfield";
  private static final String PREFIX_VALUE = "site.ycsb.db.etcd3.testvalue";

  private Etcd3Client client;

  @Before
  public void setup() throws DBException {
    client = new Etcd3Client();
    client.init();

    for (int i = 0; i < NUMBER_OF_RECORDS; i++) {
      String key = PREFIX_KEY + String.valueOf(i);
      String value = PREFIX_VALUE + String.valueOf(i);
      Map<String, ByteIterator> values = new HashMap<>();
      values.put(PREFIX_FIELD, new ByteArrayByteIterator(value.getBytes()));
      client.insert(null, key, values);
    }
  }

  @After
  public void teardown() throws DBException {
    client.clear();
    client.cleanup();
  }

  @Test
  public void insert() {
    String key = PREFIX_KEY + String.valueOf(NUMBER_OF_RECORDS);
    String value = PREFIX_VALUE + String.valueOf(NUMBER_OF_RECORDS);
    Map<String, ByteIterator> map = new HashMap<>();
    map.put(PREFIX_FIELD + "a", new ByteArrayByteIterator(value.getBytes()));
    map.put(PREFIX_FIELD + "b", new ByteArrayByteIterator(value.getBytes()));

    Status status = client.insert(PREFIX_FIELD, key, map);

    assertEquals(Status.OK, status);

    Map<String, ByteIterator> results = new HashMap<>();

    Map<String, ByteIterator> expected = new HashMap<>();
    expected.put(PREFIX_FIELD + "a", new ByteArrayByteIterator(value.getBytes()));
    expected.put(PREFIX_FIELD + "b", new ByteArrayByteIterator(value.getBytes()));

    status = client.read(null, key, null, results);

    assertEquals(Status.OK, status);
    assertEquals(expected.toString(), results.toString());
  }

  @Test
  public void read() {
    String key = PREFIX_KEY + String.valueOf(10);
    Map<String, ByteIterator> results = new HashMap<>();

    Map<String, ByteIterator> expected = new HashMap<>();
    expected.put(PREFIX_FIELD, new ByteArrayByteIterator((PREFIX_VALUE + String.valueOf(10)).getBytes()));

    Status status = client.read(null, key, null, results);

    assertEquals(Status.OK, status);
    assertEquals(expected.toString(), results.toString());

    results = new HashMap<>();

    expected = new HashMap<>();
    expected.put(PREFIX_FIELD, new ByteArrayByteIterator((PREFIX_VALUE + String.valueOf(10)).getBytes()));

    status = client.read(null, key, Set.of(PREFIX_FIELD), results);

    assertEquals(Status.OK, status);
    assertEquals(expected.toString(), results.toString());
  }

  @Test
  public void update() {
    String key = PREFIX_KEY + String.valueOf(0);
    String value = PREFIX_VALUE + "updated";
    Map<String, ByteIterator> map = new HashMap<>();
    map.put(PREFIX_FIELD, new ByteArrayByteIterator(value.getBytes()));

    Status status = client.update(null, key, map);

    assertEquals(Status.OK, status);

    Map<String, ByteIterator> results = new HashMap<>();

    Map<String, ByteIterator> expected = new HashMap<>();
    expected.put(PREFIX_FIELD, new ByteArrayByteIterator(value.getBytes()));

    status = client.read(null, key, null, results);

    assertEquals(Status.OK, status);
    assertEquals(expected.toString(), results.toString());
  }

  @Test
  public void delete() {
    String key = PREFIX_KEY + String.valueOf(0);

    Status status = client.delete(null, key);

    assertEquals(Status.OK, status);

    status = client.read(null, key, null, new HashMap<>());

    assertEquals(Status.NOT_FOUND, status);
  }
}
