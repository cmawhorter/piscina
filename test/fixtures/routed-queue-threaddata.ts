export default function (task, threadData) { return { partition: task.partition, workerPartition: threadData.partition }; }
