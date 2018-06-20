package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/*
*类名和方法不能修改
 */
public class Schedule {

	private ConcurrentHashMap<String, Task> queuingTasks = null;
	private ConcurrentHashMap<String, Task> runningTasks = null;
	private ConcurrentHashMap<String, Node> nodes = null;

    public int init() {
		queuingTasks = new ConcurrentHashMap<String, Task>();
		runningTasks = new ConcurrentHashMap<String, Task>();
		nodes = new ConcurrentHashMap<String, Node>();
		
		//初始化成功，返回E001初始化成功。
		return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {
		// 如果服务节点编号小于等于0, 返回E004:服务节点编号非法。
		if (nodeId <= 0) {
			return ReturnCodeKeys.E004;
		}

		// 如果服务节点编号已注册, 返回E005:服务节点已注册。
		String nodeKey = String.valueOf(nodeId);
		Node node = nodes.get(nodeKey);
		if (node != null) {
			return ReturnCodeKeys.E005;
		}

		// 注册成功，返回E003:服务节点注册成功。
		node = new Node(nodeId);
		nodes.put(nodeKey, node);
		return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {
		// 如果服务节点编号小于等于0, 返回E004:服务节点编号非法。
		if (nodeId <= 0) {
			return ReturnCodeKeys.E004;
		}

		// 如果服务节点编号未被注册, 返回E007:服务节点不存在。
		String nodeKey = String.valueOf(nodeId);
		Node node = nodes.remove(String.valueOf(nodeKey));
		if (node == null) {
			return ReturnCodeKeys.E007;
		}

		// 注销成功，返回E006:服务节点注销成功。
		ArrayList<Task> nodeTasks = node.runningTasks;
		for (int i = 0; i < nodeTasks.size(); i++) {
			Task task = nodeTasks.get(i);
			this.queuingTasks.put(task.taskId, task);
			this.runningTasks.remove(task.taskId);
		}
		return ReturnCodeKeys.E006;
    }


    public int addTask(int taskId, int consumption) {
		// 如果任务编号小于等于0, 返回E009:任务编号非法。
		if (taskId <= 0) {
			return ReturnCodeKeys.E009;
		}

		// 如果相同任务编号任务已经被添加, 返回E010:任务已 添加。
		String taskKey = String.valueOf(taskId);
		if(queuingTasks.get(taskKey) != null || runningTasks.get(taskKey) != null)
		{
			return ReturnCodeKeys.E010;
		}
		
		// 添加成功，返回E008任务添加成功。
		Task task = new Task(taskId, consumption);
		queuingTasks.put(taskKey, task);
		return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId) {
		// 如果任务编号小于等于0, 返回E009:任务编号非法。
		if (taskId <= 0) {
			return ReturnCodeKeys.E009;
		}

		// 如果指定编号的任务未被添加, 返回E012:任务不存在。
		String taskKey = String.valueOf(taskId);
		if (queuingTasks.get(taskKey) == null && runningTasks.get(taskKey) == null) {
			return ReturnCodeKeys.E012;
		}

		// 删除成功，返回E011:任务删除成功。
		if (queuingTasks.get(taskKey) != null) {
			queuingTasks.remove(taskKey);
			return ReturnCodeKeys.E011;
		} else {
			Task task = runningTasks.remove(taskKey);

			// 从节点上删除
			Node node = task.node;
			if (node != null) {
				ArrayList<Task> nodeTasks = node.runningTasks;
				for (int i = 0; i < nodeTasks.size(); i++) {
					Task other = nodeTasks.get(i);
					if (task.taskId.equals(other.taskId)) {
						nodeTasks.remove(i);
						node.totalLoads -= task.consumption;
						break;
					}
				}
			}

			return ReturnCodeKeys.E011;
		}
    }


    public int scheduleTask(int threshold) {
    	// threshold系统任务调度阈值，取值范围： 大于0；
    	// 如果调度阈值取值错误，返回E002调度阈值非法。
    	if (threshold <= 0)
    	{
    		return ReturnCodeKeys.E002;
    	}
    	
    	
    	// 如果获得最佳迁移方案, 进行了任务的迁移,返回E013: 任务调度成功;
    	//如果所有迁移方案中，总会有任意两台服务器的总消耗率差值大于阈值。则认为没有合适的迁移方案,返回 E014:无合适迁移方案;
        return ReturnCodeKeys.E013;
    }


    public int queryTaskStatus(List<TaskInfo> tasks) {
		// 如果查询结果参数tasks为null，返回E016:参数列表非法
		if (tasks == null) {
			return ReturnCodeKeys.E016;
		}

		// 在保存查询结果之前,要求将列表清空.
		tasks.clear();

		for (Task task : queuingTasks.values()) {
			// 如果该任务处于挂起队列中, 所属的服务编号为-1;
			tasks.add(new TaskInfo(Integer.parseInt(task.taskId), -1));
		}

		for (Task task : runningTasks.values()) {
			tasks.add(new TaskInfo(Integer.parseInt(task.taskId), Integer.parseInt(task.node.nodeId)));
		}
		
		// Tasks 保存所有任务状态列表；要求按照任务编号升序排列,
		tasks.sort(new Comparator<TaskInfo>() {

			public int compare(TaskInfo t1, TaskInfo t2) {
				return t1.getTaskId() - t2.getTaskId();
			}
			
		});

		// 如果查询成功, 返回E015: 查询任务状态成功;查询结果从参数Tasks返回。
		return ReturnCodeKeys.E015;
    }

}

class Task {
	String taskId = null;
	int consumption = 0;
	Node node = null;

	public Task(int taskId, int consumption) {
		this.taskId = String.valueOf(taskId);
		this.consumption = consumption;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Task other = (Task) obj;
		if (taskId == null) {
			if (other.taskId != null)
				return false;
		} else if (!taskId.equals(other.taskId))
			return false;
		return true;
	}
	
}

class Node {
	String nodeId = null;
	ArrayList<Task> runningTasks = new ArrayList<Task>();
	int totalLoads = 0;

	public Node(int nodeId) {
		this.nodeId = String.valueOf(nodeId);
	}
}
