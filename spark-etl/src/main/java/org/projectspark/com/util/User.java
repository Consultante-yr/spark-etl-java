package org.projectspark.com.util;


public class User {
	
	public String Name ;
	
	public Integer age ;
	
	

	public User() {
		
	}
	
	public User(String name, Integer age) {
		super();
		Name = name;
		this.age = age;
	}

	public String getName() {
		return Name;
	}

	public void setName(String name) {
		Name = name;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}



	
	
}


