package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

/*
*类名和方法不能修改
 */
public class Schedule {

	private HashMap<String, Task> allTasks = null;
	private ArrayList<Task> queuingTasks = null;
	private ArrayList<Task> runningTasks = null;
	private HashMap<String, Node> nodes = null;
	private ArrayList<TaskInfo> plan = null;

    public int init() {
    	allTasks = new HashMap<String, Task>();
		queuingTasks = new ArrayList<Task>(50);
		runningTasks = new ArrayList<Task>(50);
		nodes = new HashMap<String, Node>();
		plan = null;
		
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
			for(int j=0; j< this.runningTasks.size(); j++)
			{
				if(this.runningTasks.get(j).id == task.id)
				{
					this.runningTasks.remove(j);
					break;
				}
			}
			this.queuingTasks.add(task);
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
		if(this.allTasks.get(taskKey) != null)
		{
			return ReturnCodeKeys.E010;
		}
		
		// 添加成功，返回E008任务添加成功。
		Task task = new Task(taskId, consumption);
		queuingTasks.add(task);
		allTasks.put(taskKey, task);
		return ReturnCodeKeys.E008;
    }


	public int deleteTask(int taskId) {
		// 如果任务编号小于等于0, 返回E009:任务编号非法。
		if (taskId <= 0) {
			return ReturnCodeKeys.E009;
		}

		// 如果指定编号的任务未被添加, 返回E012:任务不存在。
		String taskKey = String.valueOf(taskId);
		if (this.allTasks.get(taskKey) == null) {
			return ReturnCodeKeys.E012;
		}

		// 删除成功，返回E011:任务删除成功。
		this.allTasks.remove(taskKey);
		for (int i = 0; i < this.queuingTasks.size(); i++) {
			if (this.queuingTasks.get(i).id == taskId) {
				this.queuingTasks.remove(i);
			}
		}
		for (int i = 0; i < this.runningTasks.size(); i++) {
			if (this.runningTasks.get(i).id == taskId) {
				this.runningTasks.remove(i);
			}
		}
		return ReturnCodeKeys.E011;
	}


    public int scheduleTask(int threshold) {
    	// threshold系统任务调度阈值，取值范围： 大于0；
    	// 如果调度阈值取值错误，返回E002调度阈值非法。
    	if (threshold <= 0)
    	{
    		return ReturnCodeKeys.E002;
    	}
    	
    	ArrayList<Task> tasks = new ArrayList<Task>(50);
    	tasks.addAll(this.runningTasks);
    	tasks.addAll(this.queuingTasks);
		tasks.sort(new Comparator<Task>() {

			public int compare(Task t1, Task t2) {
				if(t1.consumption != t2.consumption)
				{
					return t2.consumption - t1.consumption;
				}
				else
				{
					return t1.id - t2.id;
				}
			}
			
		});
		
		ArrayList<Node> nodeArray = new ArrayList<Node>(50);
		for(Node node: this.nodes.values())
		{
			nodeArray.add(new Node(node.id));
		}
		sortNodes(nodeArray);

		Task lastTask = null;
		int endIdx = nodeArray.size() -1;
		for (int i = 0; i < tasks.size(); i++) {
			// 如果迁移后，满足以上要求的方案有多个，则应选择编号小的服务器上的任务编号升序序列最小。
			Task task = tasks.get(i);
			if (lastTask != null && nodeArray.get(0).totalLoads == nodeArray.get(endIdx).totalLoads
					&& nodeArray.get(0).totalTasks == nodeArray.get(endIdx).totalTasks
					&& task.consumption == lastTask.consumption) {
				nodeArray.get(endIdx).delEndTask();
				nodeArray.get(endIdx).addTask(task);
				nodeArray.get(0).addTask(lastTask);
			} else {
				nodeArray.get(0).addTask(task);
			}
			
			sortNodes(nodeArray);
			lastTask = task;
		}
		
		
		//如果挂起队列中有任务存在，则进行根据上述的任务调度策略，获得最佳迁移方案，进行任务的迁移， 返回调度成功
		if (this.queuingTasks.size() > 0) {
			// 如果获得最佳迁移方案, 进行了任务的迁移,返回E013: 任务调度成功;
            ArrayList<TaskInfo> planB = genPlan(nodeArray);
			this.plan = planB;
			
			this.runningTasks.addAll(this.queuingTasks);
			this.queuingTasks.clear();
			
			return ReturnCodeKeys.E013;
		}
		//如果没有挂起的任务，则将运行中的任务则根据上述的任务调度策略，获得最佳迁移方案；
		else
		{
			int minLoads = nodeArray.get(0).totalLoads;
			int maxLoads = nodeArray.get(nodeArray.size()-1).totalLoads;
			
			//如果在最佳迁移方案中，任意两台不同服务节点上的任务资源总消耗率的差值小于等于调度阈值， 则进行任务的迁移，返回调度成功，
			if ((maxLoads - minLoads) <=  threshold) {
				// 如果获得最佳迁移方案, 进行了任务的迁移,返回E013: 任务调度成功;
	            ArrayList<TaskInfo> planB = genPlan(nodeArray);
				this.plan = planB;
				return ReturnCodeKeys.E013;
			}
			//如果在最佳迁移方案中，任意两台不同服务节点上的任务资源总消耗率的差值大于调度阈值，则不做任务的迁移，返回无合适迁移方案
			else
			{
				//如果所有迁移方案中，总会有任意两台服务器的总消耗率差值大于阈值。则认为没有合适的迁移方案,返回 E014:无合适迁移方案;
				return ReturnCodeKeys.E014;
			}
		}
    }
    
	private void sortNodes(ArrayList<Node> nodeArray) {
		nodeArray.sort(new Comparator<Node>() {

			public int compare(Node n1, Node n2) {
				if (n1.totalLoads != n2.totalLoads) {
					return n1.totalLoads - n2.totalLoads;
				} else if (n1.totalTasks != n2.totalTasks) {
					return n1.totalTasks - n2.totalTasks;
				} else {
					return n1.id - n2.id;
				}
			}

		});
		
		// printNodes(nodeArray);
	}
	
	protected void printNodes(ArrayList<Node> nodeArray) {
		System.out.println("--------------------");
		for (Node node : nodeArray) {
			String log = "";
			for (Task task : node.runningTasks) {
				log += task.taskId + "(" + task.consumption + "),";
			}
			System.out.println(node.nodeId + "\t" + node.totalLoads + "\t" + node.totalTasks + "\t" + log);
		}
	}
	
	private ArrayList<TaskInfo> genPlan(ArrayList<Node> nodeArray) {
		ArrayList<TaskInfo> planB = new ArrayList<TaskInfo>(50);
		for (Node node : nodeArray) {
			for (Task task : node.runningTasks) {
				planB.add(new TaskInfo(task.id, node.id));

			}
		}
		planB.sort(new Comparator<TaskInfo>() {

			public int compare(TaskInfo t1, TaskInfo t2) {
				return t1.getTaskId() - t2.getTaskId();
			}
		});
		return planB;
	}


    public int queryTaskStatus(List<TaskInfo> tasks) {
		// 如果查询结果参数tasks为null，返回E016:参数列表非法
		if (tasks == null) {
			return ReturnCodeKeys.E016;
		}

		// 在保存查询结果之前,要求将列表清空.
		tasks.clear();
		
		if(this.plan != null)
		{
			tasks.addAll(this.plan);
		}
		
		// 如果查询成功, 返回E015: 查询任务状态成功;查询结果从参数Tasks返回。
		return ReturnCodeKeys.E015;
    }

}

class Task {
	int id = -1;
	String taskId = null;
	int consumption = 0;

	public Task(int taskId, int consumption) {
		this.id = taskId;
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
	int id = -1;
	String nodeId = null;
	ArrayList<Task> runningTasks = new ArrayList<Task>();
	int totalLoads = 0;
	int totalTasks = 0;

	public Node(int nodeId) {
		this.id = nodeId;
		this.nodeId = String.valueOf(nodeId);
	}
	
	public void addTask(Task t)
	{
		runningTasks.add(t);
		this.totalLoads += t.consumption;
		this.totalTasks ++;
	}
	
	public Task delEndTask()
	{
		Task endTask = runningTasks.remove(runningTasks.size()-1);
		this.totalLoads -= endTask.consumption;
		this.totalTasks --;
		return endTask;
	}
	
}
