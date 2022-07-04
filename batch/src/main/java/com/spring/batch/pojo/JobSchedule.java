package com.spring.batch.pojo;

public class JobSchedule {

	int id;
	String jobGroup;
	String jobName;
	String cronExpression;
	
	public JobSchedule() {}

	public JobSchedule(int id, String jobGroup, String jobName, String cronExpression) {
		super();
		this.id = id;
		this.jobGroup = jobGroup;
		this.jobName = jobName;
		this.cronExpression = cronExpression;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getJobGroup() {
		return jobGroup;
	}

	public void setJobGroup(String jobGroup) {
		this.jobGroup = jobGroup;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getCronExpression() {
		return cronExpression;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}
}
