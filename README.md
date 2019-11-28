
# forked from [ltsopensource/light-task-scheduler](https://github.com/ltsopensource/light-task-scheduler)
# 自定义修改
## 说明
主要是因为原框架里面TaskTracker里面的对多种任务支持不是很好,因为对多种任务的时候,会存在任务线程数也有控制,这时候这块就控制的不是很好,因为原有的是公用一个线程池的,所以增加了一个子任务节点的属性,当然如果碰到那种又要多任务的又要有线程控制的,可以部署多个TaskTracker,命名不同的type,但是这样会导致节点句多,同时一些共有的组件库如果修改的话,需要发布相关依赖的tracker.








