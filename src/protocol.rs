use serde::Deserialize;
use std::collections::HashMap;
use std::vec::Vec;

pub struct GetRouteInfoRequestHeader {
    topic: String,
}

impl GetRouteInfoRequestHeader {
    pub fn new(topic: &str) -> Self {
        Self {
            topic: topic.to_owned(),
        }
    }
}

impl From<GetRouteInfoRequestHeader> for HashMap<String, String> {
    fn from(header: GetRouteInfoRequestHeader) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("topic".to_owned(), header.topic);
        map
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueData {
    broker_name: String,
    read_queue_nums: i32,
    write_queue_nums: i32,
    perm: i32,
    topic_syn_flag: i32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerData {
    cluster: String,
    broker_name: String,
    broker_addrs: HashMap<i64, String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicRouteData {
    order_topic_conf: Option<String>,
    queue_datas: Vec<QueueData>,
    broker_datas: Vec<BrokerData>,

    // deprecated
    filter_server_table: HashMap<String, Vec<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_get_route_info_request_header() {
        let header = GetRouteInfoRequestHeader::new("Test");
        let map: HashMap<String, String> = header.into();
        assert_eq!(map.len(), 1);
        assert_eq!(Some(&String::from("Test")), map.get("topic"));
    }

    #[test]
    fn test_queue_data_deserialization() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"
        {"brokerName":"b1","perm":1,"readQueueNums":8,"topicSynFlag":2,"writeQueueNums":6}
        "#;
        let queue_data: QueueData = serde_json::from_str(json)?;
        assert_eq!(queue_data.broker_name, "b1");
        assert_eq!(queue_data.perm, 1);
        assert_eq!(queue_data.read_queue_nums, 8);
        assert_eq!(queue_data.write_queue_nums, 6);
        assert_eq!(queue_data.topic_syn_flag, 2);
        Ok(())
    }

    #[test]
    fn test_broker_data_deserialization() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"
        {"brokerAddrs":{"0":"localhost:8888","1":"localhost:1234"},"brokerName":"b1","cluster":"C1","enableActingMaster":false}
        "#;
        let broker_data: BrokerData = serde_json::from_str(json)?;
        assert_eq!(broker_data.broker_name, "b1");
        assert_eq!(broker_data.cluster, "C1");
        assert_eq!(broker_data.broker_addrs.len(), 2);
        Ok(())
    }

    #[test]
    fn test_topic_route_data_deserialization() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"
        {"brokerDatas":[{"brokerAddrs":{"0":"localhost:8888","1":"localhost:1234"},"brokerName":"b1","cluster":"C1","enableActingMaster":false}],"filterServerTable":{},"queueDatas":[{"brokerName":"b1","perm":1,"readQueueNums":8,"topicSynFlag":2,"writeQueueNums":6}]}
        "#;
        let topic_route_data: TopicRouteData = serde_json::from_str(json)?;
        assert_eq!(topic_route_data.order_topic_conf, None);
        assert_eq!(topic_route_data.broker_datas.len(), 1);
        assert_eq!(topic_route_data.queue_datas.len(), 1);
        if let Some(broker_data) = topic_route_data.broker_datas.first() {
            assert_eq!(broker_data.broker_name, "b1");
        }

        if let Some(queue_data) = topic_route_data.queue_datas.first() {
            assert_eq!(queue_data.broker_name, "b1");
        }

        Ok(())
    }
}
