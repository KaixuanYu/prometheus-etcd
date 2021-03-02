//https://prometheus.io/blog/2015/08/17/service-discovery-with-etcd/
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"go.etcd.io/etcd/mvcc/mvccpb"

	"go.etcd.io/etcd/clientv3"
	"golang.org/x/net/context"
)

type TargetGroup struct {
	Targets []string          `json:"targets,omitempty"`
	Labels  map[string]string `json:"labels,omitempty"`
}

type (
	instances map[string]string    //"ip:port":"ip:port"
	services  map[string]instances //"classroom":["ip:port":"ip:port","ip:port":"ip:port"]
)

const servicesPrefix = "/services"

var (
	pathPat     = regexp.MustCompile(servicesPrefix + `/([^/]+)(?:/([^/]+))`) //([^/]+) 匹配并获取到下一个/符号的任意字符的集合。
	targetFile  = flag.String("file", "tg_group.json", "target file path")
	dialTimeout = time.Duration(*flag.Int("dial-timeout", 5, "dial etcd timeout")) * time.Second
	endpoints   = flag.String("endpoints", "127.0.0.1:2379", "etcd endpoints")
)

func (ss services) persist() {
	var groups []TargetGroup
	// Write files for current services.
	for job, instances := range ss {
		var targets []string
		for _, addr := range instances {
			targets = append(targets, addr)
		}

		groups = append(groups, TargetGroup{
			Targets: targets,
			Labels:  map[string]string{"job": job},
		})
	}

	content, err := json.Marshal(groups)
	if err != nil {
		log.Fatal(err)
		return
	}

	f, err := os.Create(*targetFile)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer func() {
		_ = f.Close()
	}()

	if _, err := f.Write(content); err != nil {
		log.Fatal(err)
	}
}

func (ss services) update(srv, instance, value string) {
	ins, ok := ss[srv]
	if !ok {
		ins = instances{}
		ss[srv] = ins
	}
	ins[instance] = value
}

func (ss services) delete(srv, instance, value string) {
	if ss[srv][instance] != "" {
		delete(ss[srv], instance)
	}
}

func (ss *services) handle(key, value []byte, handler func(srv, instances, value string)) {
	match := pathPat.FindStringSubmatch(string(key))
	if len(match) != 3 {
		log.Fatal(errors.New("key is not valid. must compile " + servicesPrefix + "/service/host:port"))
		return
	}
	srv, instance := match[1], match[2]
	handler(srv, instance, string(value))
}

func main() {
	flag.Parse()
	var (
		ss  = services{}
		eps = strings.Split(*endpoints, ",")
	)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   eps,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = cli.Close()
	}()

	go func() {
		//key:servicesPrefix + /{service_name}/ip:port   value:ip:port
		rch := cli.Watch(context.Background(), servicesPrefix, clientv3.WithPrefix())
		for resp := range rch {
			for _, ev := range resp.Events {
				log.Printf("Watch: %s %q: %q \n", ev.Type, ev.Kv.Key, ev.Kv.Value)

				handler := ss.update
				if ev.Type == mvccpb.DELETE {
					handler = ss.delete
				}
				ss.handle(ev.Kv.Key, ev.Kv.Value, handler)
			}
			ss.persist()
		}
	}()
	<-make(chan int)
}
