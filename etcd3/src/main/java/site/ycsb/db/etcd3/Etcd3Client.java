/**
 * Copyright (c) 2025 José Marcelo Marques da Cruz.
 */

package site.ycsb.db.etcd3;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 *
 */
public class Etcd3Client extends DB {
  private static final String FIELD_SEPARATOR = ".";

  private Client client;

  @Override
  public void init() throws DBException {
    client = Client.builder().endpoints("http://localhost:2379").build();
  }

  @Override
  public void cleanup() throws DBException {
    client.close();
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    KV kv = client.getKVClient();

    GetResponse response = null;
    Status status = Status.NOT_FOUND;
    if (fields == null) {
      ByteSequence k = ByteSequence.from(key.getBytes());
      GetOption opt = GetOption.builder()
          .isPrefix(true)
          .build();

      try {
        response = kv.get(k, opt).get();
      } catch (InterruptedException | ExecutionException e) {
        return Status.ERROR;
      }

      for (KeyValue entry: response.getKvs()) {
        ByteSequence field = entry.getKey().substring(k.size() + FIELD_SEPARATOR.length());
        ByteSequence v = entry.getValue();

        result.put(field.toString(), new ByteArrayByteIterator(v.getBytes()));

        status = Status.OK;
      }
    } else {
      for (String field: fields) {
        ByteSequence k = ByteSequence.from((key + FIELD_SEPARATOR + field).getBytes());

        try {
          response = kv.get(k).get();
        } catch (InterruptedException | ExecutionException e) {
          return Status.ERROR;
        }

        if (response.getKvs().isEmpty()) {
          continue;
        }

        ByteSequence v = response.getKvs().get(0).getValue();
        result.put(field, new ByteArrayByteIterator(v.getBytes()));

        status = Status.OK;
      }
    }

    return status;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    KV kv = client.getKVClient();

    for (Map.Entry<String, ByteIterator> entry: values.entrySet()) {
      ByteSequence k = ByteSequence.from((key + FIELD_SEPARATOR + entry.getKey()).getBytes());
      ByteSequence v = ByteSequence.from(entry.getValue().toArray());

      try {
        kv.put(k, v).get();
      } catch (InterruptedException | ExecutionException e) {
        return Status.ERROR;
      }
    }

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return update(table, key, values);
  }

  @Override
  public Status delete(String table, String key) {
    KV kv = client.getKVClient();

    ByteSequence k = ByteSequence.from(key.getBytes());
    DeleteOption opt = DeleteOption.builder()
        .isPrefix(true)
        .build();
    try {
      kv.delete(k, opt).get();
    } catch (InterruptedException | ExecutionException e) {
      return Status.ERROR;
    }

    return Status.OK;
  }

  public Status clear() {
    ByteSequence key = ByteSequence.from(new byte[]{'\0'});
    DeleteOption opt = DeleteOption.builder()
        .withRange(ByteSequence.from(new byte[]{'\0'}))
        .build();
    try {
      client.getKVClient().delete(key, opt).get();
    } catch (InterruptedException | ExecutionException e) {
      return Status.ERROR;
    }

    return Status.OK;
  }
}
