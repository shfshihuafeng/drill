package org.apache.drill.exec.store.hive.readers.filter;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hive.com.esotericsoftware.kryo.Kryo;
import org.apache.hive.com.esotericsoftware.kryo.io.Output;

public class HiveFilter {
  private SearchArgument searchArgument;

  public HiveFilter(SearchArgument searchArgument) {
    this.searchArgument = searchArgument;
  }

  public SearchArgument getSearchArgument() {
    return searchArgument;
  }

  public String getSearchArgumentString() {
    return toKryo(searchArgument);
  }

  private static String toKryo(SearchArgument sarg) {
    Output out = new Output(4 * 1024, 10 * 1024 * 1024);
    new Kryo().writeObject(out, sarg);
    out.close();
    return Base64.encodeBase64String(out.toBytes());
  }
}
