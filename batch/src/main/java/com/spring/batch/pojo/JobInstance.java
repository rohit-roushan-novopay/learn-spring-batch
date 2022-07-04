package com.spring.batch.pojo;

public class JobInstance {

	private int id;
	private int version;
	
	private String jobName;
	private String jobKey;
	
	@Override
	public String toString() {
		return "JobInstance [id=" + id + ", version=" + version + ", jobName=" + jobName + ", jobKey=" + jobKey + "]";
	}

	public JobInstance() {}

	public JobInstance(int id, int version, String jobName, String jobKey) {
		super();
		this.id = id;
		this.version = version;
		this.jobName = jobName;
		this.jobKey = jobKey;
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getVersion() {
		return version;
	}
	public void setVersion(int version) {
		this.version = version;
	}
	public String getJobName() {
		return jobName;
	}
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}
	public String getJobKey() {
		return jobKey;
	}
	public void setJobKey(String jobKey) {
		this.jobKey = jobKey;
	}
}
