// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package serial_suite
import ( 
 	"bytes" 
 	"fmt" 
 	"os/exec" 
 	"time" 
 
 
 	"github.com/ngaut/log" 
 	"github.com/pingcap/octopus/stability-tester/config" 
 	"golang.org/x/net/context" 
) 

func init(){
        RegisterSuite("sqllogic",NewSqllogicTest)
}

type SqllogicTest struct{
        cfg      *config.SqllogicConfig
        host     string
        port     int
        user     string
        password string
}

func NewSqllogicTest(cfg *config.Config) Case{
        return &SqllogicTest{
                cfg:      &cfg.SerialSuite.Sqllogic,
                host:     cfg.Host,
                port:     cfg.Port,
                user:     cfg.User,
                password: cfg.Password,
        }
}

func (s *SqllogicTest) String() string{
        return "sqllogic"
}

func (s *SqllogicTest) Initialize() error{
        err := s.clean()
        if err != nil{
                return err
        }
        return nil
}

func (s *SqllogicTest) Execute(ctx context.Context) error { 
 	err := s.runAction() 
 	if err != nil { 
 		return err 
 	} 
 	ticker := time.NewTicker(s.cfg.Interval.Duration) 
 	defer ticker.Stop() 
 	for { 
 		select { 
 		case <-ctx.Done(): 
 			return nil 
 		case <-ticker.C: 
 			err := s.runAction() 
 			if err != nil { 
 				return err 
 			} 
 		} 
	}
 	return nil 
}
 
func (s *SqllogicTest) runAction() error {  
	if err := s.run(); err != nil { 
 		return err 
 	}
	msg :="sqllogic is done, begin to clean..."
        log.Infof("msg: %s", msg) 
 	if err := s.clean(); err != nil { 
 		return err 
 	} 
 	return nil 
}
 
func (s *SqllogicTest) run() error { 
 	cmdStr := fmt.Sprintf(`%s -tp %s -host %s -port %d -log-level %s -skip-error %s`, 
 		s.cfg.Binpath, s.cfg.Testpath,s.host, s.port, s.cfg.LogLevel,s.cfg.SkipError) 
 	cmd := exec.Command("/bin/sh", "-c", cmdStr) 
 	log.Infof("run command: %s", cmdStr) 
 
 	var out bytes.Buffer 
 	//cmd.Stdout = &out
	cmd.Stderr = &out 
 	if err := cmd.Run(); err != nil { 
 		log.Errorf("%s\n", out.String()) 
 		return err 
 	} 
 
 	return nil 
} 
  
func (s *SqllogicTest) clean() error { 
 	cmdStrArgs := fmt.Sprintf(`/usr/bin/mysql -h%s -P%d -u%s -e"drop database if exists sqllogic_test"`, s.host, s.port, s.user) 
 	fmt.Println("clean command: ", cmdStrArgs) 
 	cmd := exec.Command("/bin/sh", "-c", cmdStrArgs) 
 	if err := cmd.Run(); err != nil { 
 		fmt.Println("run drop database failed", err) 
 		return err 
 	} 
 	return nil 
} 




