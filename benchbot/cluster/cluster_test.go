package cluster

import (
	"context"
	"log"
	"testing"
)

func TestCluster(t *testing.T) {
	dsn := &ClusterDSN{
		Name:     "test",
		Host:     "127.0.0.1",
		Port:     4000,
		DB:       "test",
		User:     "root",
		Password: "",
	}
	cfg := &AnsibleConfig{
		Dir:      "ansible",
		Clusters: []*ClusterDSN{dsn},
	}

	ctx := context.Background()
	cm, err := NewClusterManager(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	c, err := cm.RequireCluster(nil, nil, nil)
	if err != nil {
		log.Fatal(err)
	}

	_, err = cm.RequireCluster(nil, nil, nil)
	if err == nil {
		log.Fatal("should return error")
	}

	c.Close()

	_, err = cm.RequireCluster(nil, nil, nil)
	if err != nil {
		log.Fatal(err)
	}
}
