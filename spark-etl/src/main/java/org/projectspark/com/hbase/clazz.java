package org.projectspark.com.hbase;

import java.io.Serializable;

public class clazz implements Serializable{
	  private String rowkey;
	  private String Colonne_qualifier1;
	  private String Colonne_qualifier2;
	  
	  private Integer age ;
	  

	  public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public clazz() {
	  }

	  public String getRowkey() {
	    return rowkey;
	  }

	  public void setRowkey(String rowkey) {
	    this.rowkey = rowkey;
	  }

	  public String getFirstName() {
	    return Colonne_qualifier1;
	  }

	  public void setFirstName(String firstName) {
	    this.Colonne_qualifier1 = firstName;
	  }

	  public String getLastName() {
	    return Colonne_qualifier2;
	  }

	  public void setLastName(String lastName) {
	    this.Colonne_qualifier2 = lastName;
	  }

	  @Override
	  public String toString() {
	    return "User{" +
	            "rowkey='" + rowkey + '\'' +
	            ", firstName='" + Colonne_qualifier1 + '\'' +
	            ", lastName='" + Colonne_qualifier2 + '\'' +
	            '}';
	  }
}
