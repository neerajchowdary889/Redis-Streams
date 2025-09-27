package Config

import (
    "os"
    "sync"
    "gopkg.in/yaml.v3"
)

// Topic represents each topic entry
type Topic struct {
    Name          string `yaml:"name"`
    StreamName    string `yaml:"stream_name"`
    ConsumerGroup string `yaml:"consumer_group"`
    ConsumerName  string `yaml:"consumer_name"`  // Add this field
}

// LoadTopicConfig represents the root of your YAML
type LoadTopicConfig struct {
    Topics []Topic `yaml:"topics"`
}

var (
    cfg  *LoadTopicConfig
    once sync.Once
)

// TopicConfigLoader loads and caches the config only once
func TopicConfigLoader(filename string) (*LoadTopicConfig, error) {
    var loadErr error
    once.Do(func() {
        var c LoadTopicConfig
        data, e := os.ReadFile(filename)
        if e != nil {
            loadErr = e
            return
        }
        if e := yaml.Unmarshal(data, &c); e != nil {
            loadErr = e
            return
        }
        cfg = &c
    })
    return cfg, loadErr
}

// GetStreamName returns stream name for a topic
func (c *LoadTopicConfig) GetStreamName(topicName string) string {
    for _, t := range c.Topics {
        if t.Name == topicName {
            return t.StreamName
        }
    }
    return ""
}

// GetConsumerGroup returns consumer group for a topic
func (c *LoadTopicConfig) GetConsumerGroup(topicName string) string {
    for _, t := range c.Topics {
        if t.Name == topicName {
            return t.ConsumerGroup
        }
    }
    return ""
}

// GetConsumerName returns consumer name for a topic
func (c *LoadTopicConfig) GetConsumerName(topicName string) string {
    for _, t := range c.Topics {
        if t.Name == topicName {
            return t.ConsumerName
        }
    }
    return ""
}

// GetStreamConsumerPairs returns topic_name -> {stream_name, consumer_group, consumer_name} mapping
func (c *LoadTopicConfig) GetStreamConsumerPairs() map[string]map[string]string {
    m := make(map[string]map[string]string)
    for _, t := range c.Topics {
        m[t.Name] = map[string]string{  // Use t.Name as key, not t.StreamName
            "stream_name":    t.StreamName,
            "consumer_group": t.ConsumerGroup,
            "consumer_name":  t.ConsumerName,
        }
    }
    return m
}