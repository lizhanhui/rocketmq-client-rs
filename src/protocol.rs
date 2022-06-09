//!
//! Define protocols used when talking to Apache RocketMQ servers.
//! 
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
    pub(crate) broker_name: String,
    pub(crate) read_queue_nums: i32,
    pub(crate) write_queue_nums: i32,
    pub(crate) perm: i32,
    pub(crate) topic_syn_flag: i32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerData {
    pub(crate) cluster: String,
    pub(crate) broker_name: String,
    pub(crate) broker_addrs: HashMap<i64, String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicRouteData {
    pub(crate) order_topic_conf: Option<String>,

    pub(crate) queue_datas: Vec<QueueData>,

    pub(crate) broker_datas: Vec<BrokerData>,

    // deprecated
    pub(crate) filter_server_table: HashMap<String, Vec<String>>,
}

#[derive(Debug)]
pub(crate) struct SendMessageRequestHeader {
    producer_group: String,

    topic: String,

    default_topic: String,

    default_topic_queue_nums: i32,

    queue_id: i32,

    sys_flag: i32,

    born_timestamp: i64,

    flag: i32,

    properties: Option<String>,

    reconsume_times: Option<i32>,

    unit_mode: Option<bool>,

    batch: Option<bool>,

    max_reconsume_times: Option<i32>,
}

impl From<SendMessageRequestHeader> for HashMap<String, String> {
    fn from(header: SendMessageRequestHeader) -> Self {
        let mut map = HashMap::new();
        map.insert("producerGroup".to_owned(), header.producer_group);
        map.insert("topic".to_owned(), header.topic);
        map.insert("defaultTopic".to_owned(), header.default_topic);
        map.insert(
            "defaultTopicQueueNums".to_owned(),
            format!("{}", header.default_topic_queue_nums),
        );
        map.insert("queueId".to_owned(), format!("{}", header.queue_id));
        map.insert("sysFlag".to_owned(), format!("{}", header.sys_flag));
        map.insert(
            "bornTimestamp".to_owned(),
            format!("{}", header.born_timestamp),
        );
        map.insert("flag".to_owned(), format!("{}", header.flag));
        if let Some(properties) = header.properties {
            map.insert("properties".to_owned(), properties);
        }

        if let Some(reconsume_times) = header.reconsume_times {
            map.insert("reconsumeTimes".to_owned(), format!("{}", reconsume_times));
        }

        if let Some(unit_mode) = header.unit_mode {
            map.insert("unitMode".to_owned(), format!("{}", unit_mode));
        }

        if let Some(batch) = header.batch {
            map.insert("batch".to_owned(), format!("{}", batch));
        }

        if let Some(max_reconsume_times) = header.max_reconsume_times {
            map.insert(
                "maxReconsumeTimes".to_owned(),
                format!("{}", max_reconsume_times),
            );
        }

        map
    }
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
