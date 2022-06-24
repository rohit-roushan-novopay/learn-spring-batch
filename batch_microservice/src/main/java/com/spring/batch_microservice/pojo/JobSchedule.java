package com.spring.batch_microservice.pojo;

public class JobSchedule {

	int id;
	String jobName;
	String schedule;
	
	public JobSchedule(int id, String jobName, String schedule) {
		super();
		this.id = id;
		this.jobName = jobName;
		this.schedule = schedule;
	}

	public JobSchedule() {}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getSchedule() {
		return schedule;
	}

	public void setSchedule(String schedule) {
		this.schedule = schedule;
	}
}
