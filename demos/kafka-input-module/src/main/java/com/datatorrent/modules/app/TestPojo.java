package com.datatorrent.modules.app;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

public class TestPojo implements Serializable
{
  private String lastname = "surname";
  private long rowId;
  @Bind(JavaSerializer.class)
  private UUID id;
  private boolean test;
  private double doubleValue;
  private float floatValue;
  private Date last_visited;
  private int age = 20;
  private byte[] byteData;
  private final transient Random random = new Random();

  public TestPojo()
  {

  }

  public TestPojo(long rowId)
  {
    this.rowId = rowId;
  }

  public TestPojo(UUID id, int age, String lastname, boolean test, float floatValue, double doubleValue, Date date, int byteSize)
  {
    this.id = id;
    this.age = age;
    this.lastname = lastname;
    this.test = test;
    this.floatValue = floatValue;
    this.doubleValue = doubleValue;
    this.last_visited = date;
    this.byteData = new byte[byteSize];
    random.nextBytes(this.byteData);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TestPojo testPojo = (TestPojo)o;

    if (rowId != testPojo.rowId) {
      return false;
    }
    if (test != testPojo.test) {
      return false;
    }
    if (Double.compare(testPojo.doubleValue, doubleValue) != 0) {
      return false;
    }
    if (Float.compare(testPojo.floatValue, floatValue) != 0) {
      return false;
    }
    if (age != testPojo.age) {
      return false;
    }
    if (lastname != null ? !lastname.equals(testPojo.lastname) : testPojo.lastname != null) {
      return false;
    }
    if (id != null ? !id.equals(testPojo.id) : testPojo.id != null) {
      return false;
    }
    if (last_visited != null ? !last_visited.equals(testPojo.last_visited) : testPojo.last_visited != null) {
      return false;
    }
    if (!Arrays.equals(byteData, testPojo.byteData)) {
      return false;
    }
    return !(random != null ? !random.equals(testPojo.random) : testPojo.random != null);

  }

  @Override
  public int hashCode()
  {
    int result;
    long temp;
    result = lastname != null ? lastname.hashCode() : 0;
    result = 31 * result + (int)(rowId ^ (rowId >>> 32));
    result = 31 * result + (id != null ? id.hashCode() : 0);
    result = 31 * result + (test ? 1 : 0);
    temp = Double.doubleToLongBits(doubleValue);
    result = 31 * result + (int)(temp ^ (temp >>> 32));
    result = 31 * result + (floatValue != +0.0f ? Float.floatToIntBits(floatValue) : 0);
    result = 31 * result + (last_visited != null ? last_visited.hashCode() : 0);
    result = 31 * result + age;
    result = 31 * result + Arrays.hashCode(byteData);
    result = 31 * result + (random != null ? random.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "TestPojo{" +
      "lastname='" + lastname + '\'' +
      ", rowId=" + rowId +
      ", id=" + id +
      ", test=" + test +
      ", doubleValue=" + doubleValue +
      ", floatValue=" + floatValue +
      ", last_visited=" + last_visited +
      ", age=" + age +
      ", byteData=" + Arrays.toString(byteData) +
      '}';
  }

  public long getRowId()
  {
    return rowId;
  }

  public void setRowId(long rowId)
  {
    this.rowId = rowId;
  }

  public int getAge()
  {
    return age;
  }

  public void setAge(int age)
  {
    this.age = age;
  }

  public boolean isTest()
  {
    return test;
  }

  public void setTest(boolean test)
  {
    this.test = test;
  }

  public double getDoubleValue()
  {
    return doubleValue;
  }

  public void setDoubleValue(double doubleValue)
  {
    this.doubleValue = doubleValue;
  }

  public float getFloatValue()
  {
    return floatValue;
  }

  public void setFloatValue(float floatValue)
  {
    this.floatValue = floatValue;
  }

  public Date getLast_visited()
  {
    return last_visited;
  }

  public void setLast_visited(Date last_visited)
  {
    this.last_visited = last_visited;
  }

  public UUID getId()
  {
    return id;
  }

  public void setId(UUID id)
  {
    this.id = id;
  }

  public String getLastname()
  {
    return lastname;
  }

  public void setLastname(String lastname)
  {
    this.lastname = lastname;
  }

  public byte[] getByteData()
  {
    return byteData;
  }

  public void setByteData(byte[] byteData)
  {
    this.byteData = byteData;
  }
}
