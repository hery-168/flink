package org.apache.flink.client.demo;

import org.apache.flink.client.cli.CliFrontend;

/**
 * @ClassName CLiDemo
 * @Description TODO
 * @Date 2021/4/25 17:57
 * @Author yongheng
 * @Version V1.0
 **/
public class CLiDemo {

	public static void main(String[] args) {
//                System.out.println(System.getenv("HADOOP_CLASSPATH"));
		String[] param = new String[9];
		param[0] = "run";
		param[1] = "-t";
		param[2] = "yarn-per-job";
		param[3] = "-c";
		param[4] = "com.xy.core.wc.WordCount";
		param[5] = "-j";
		param[6] = "C:\\soft\\bigdata\\flink-1.12.0\\examples\\batch\\WordCount.jar";
		param[7] = "-D";
		param[8] = "yarn.application.name=datamiddle-test-submit-code-0425-test1";
		CliFrontend.main(param);
	}

}
