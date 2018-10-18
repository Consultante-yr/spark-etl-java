package org.projectspark.com.hbase;


import scala.Serializable;

public class User implements Serializable {

  private String rowkey;
  private String firstName;
  private String lastName;
  
  private Integer age ;
  

  public Integer getAge() {
	return age;
}

public void setAge(Integer age) {
	this.age = age;
}

public User() {
  }

  public String getRowkey() {
    return rowkey;
  }

  public void setRowkey(String rowkey) {
    this.rowkey = rowkey;
  }

  public String getFirstName() {
    return firstName;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  @Override
  public String toString() {
    return "User{" +
            "rowkey='" + rowkey + '\'' +
            ", firstName='" + firstName + '\'' +
            ", lastName='" + lastName + '\'' +
            '}';
  }
}