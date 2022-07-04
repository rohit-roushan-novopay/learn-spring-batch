package com.spring.batch.pojo;

import java.util.Date;

public class JobExecution {

	private int id;
	private int job_instance_id;
	private int version;
	
	private Date create_time;
	private Date start_time;
	private Date end_time;
	
	private String status;
	private String exit_code;
	private String exit_message;
	
	private Date last_updated;
	private String job_configuration_location;
	
	@Override
	public String toString() {
		return "JobExecution [id=" + id + ", job_instance_id=" + job_instance_id + ", version=" + version
				+ ", create_time=" + create_time + ", start_time=" + start_time + ", end_time=" + end_time + ", status="
				+ status + ", exit_code=" + exit_code + ", exit_message=" + exit_message + ", last_updated="
				+ last_updated + ", job_configuration_location=" + job_configuration_location + "]";
	}

	public JobExecution() {}

	public JobExecution(int id, int version, int job_instance_id, Date create_time, Date start_time, Date end_time,
			String status, String exit_code, String exit_message, Date last_updated,
			String job_configuration_location) {
		super();
		this.id = id;
		this.job_instance_id = job_instance_id;
		this.version = version;
		this.create_time = create_time;
		this.start_time = start_time;
		this.end_time = end_time;
		this.status = status;
		this.exit_code = exit_code;
		this.exit_message = exit_message;
		this.last_updated = last_updated;
		this.job_configuration_location = job_configuration_location;
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getJob_instance_id() {
		return job_instance_id;
	}
	public void setJob_instance_id(int job_instance_id) {
		this.job_instance_id = job_instance_id;
	}
	public int getVersion() {
		return version;
	}
	public void setVersion(int version) {
		this.version = version;
	}
	public Date getCreate_time() {
		return create_time;
	}
	public void setCreate_time(Date create_time) {
		this.create_time = create_time;
	}
	public Date getStart_time() {
		return start_time;
	}
	public void setStart_time(Date start_time) {
		this.start_time = start_time;
	}
	public Date getEnd_time() {
		return end_time;
	}
	public void setEnd_time(Date end_time) {
		this.end_time = end_time;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getExit_code() {
		return exit_code;
	}
	public void setExit_code(String exit_code) {
		this.exit_code = exit_code;
	}
	public String getExit_message() {
		return exit_message;
	}
	public void setExit_message(String exit_message) {
		this.exit_message = exit_message;
	}
	public Date getLast_updated() {
		return last_updated;
	}
	public void setLast_updated(Date last_updated) {
		this.last_updated = last_updated;
	}
	public String getJob_configuration_location() {
		return job_configuration_location;
	}
	public void setJob_configuration_location(String job_configuration_location) {
		this.job_configuration_location = job_configuration_location;
	}
}
