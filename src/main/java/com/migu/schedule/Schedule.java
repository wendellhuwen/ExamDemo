package com.migu.schedule;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/*
*类名和方法不能修改
 */
public class Schedule {

	private ConcurrentHashMap<String, String> tasks = null;
	private ConcurrentHashMap<String, String> nodes = null;

	public int init() {
		// TODO 方法未实现
		tasks = new ConcurrentHashMap<String, String>();
		nodes = new ConcurrentHashMap<String, String>();
		return ReturnCodeKeys.E000;
	}

	public int registerNode(int nodeId) {
		// TODO 方法未实现
		nodes.put(String.valueOf(nodeId), "");
		return ReturnCodeKeys.E000;
	}

	public int unregisterNode(int nodeId) {
		// TODO 方法未实现
		String val = nodes.remove(String.valueOf(nodeId));
		if (val != null)
		{
			return ReturnCodeKeys.E006;
		} else {
			return ReturnCodeKeys.E007;
		}
	}

	public int addTask(int taskId, int consumption) {
		// TODO 方法未实现
		return ReturnCodeKeys.E000;
	}

	public int deleteTask(int taskId) {
		// TODO 方法未实现
		return ReturnCodeKeys.E011;
	}

	public int scheduleTask(int threshold) {
		// TODO 方法未实现
		return ReturnCodeKeys.E000;
	}

	public int queryTaskStatus(List<TaskInfo> tasks) {
		// TODO 方法未实现
		return ReturnCodeKeys.E000;
	}

}
