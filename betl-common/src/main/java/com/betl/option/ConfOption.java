/**
 * @Email:1768880751@qq.com
 * @Author:zhl
 * @Date:2016年1月22日下午5:21:03
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.option;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfOption {
	static Logger LOG = LoggerFactory.getLogger(ConfOption.class);

	public static Options buildOptions() {
		Options options = new Options();
		//外部传入的公共文件
		Option P = OptionBuilder.withArgName("P").hasArgs().withDescription("external public properties filename.").create("P");
		P.setRequired(true);
		options.addOption(P);
		//外部传入的私有文件
		Option p = OptionBuilder.withArgName("p").hasArgs().withDescription("external detail properties filename.").create("p");
		P.setRequired(true);
		options.addOption(p);
		//终极key=value覆盖
		options.addOption(OptionBuilder.withArgName("property=value").hasArgs(2).withValueSeparator().withDescription("use value for given property.").create("D"));
		return options;
	}

	@SuppressWarnings("static-access")
	public static Configuration getConf(String[] args) {
		Properties props = new Properties();
		Options options = buildOptions();
		Configuration conf = new Configuration();
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(200);
		try {
			CommandLineParser parser = new PosixParser();
			CommandLine cmd = parser.parse(options, args);
			if (cmd.hasOption("P")) {
				File file = new File(cmd.getOptionValue("P"));
				if (!file.exists()) {
					props.load(ConfOption.class.getClassLoader().getResourceAsStream(cmd.getOptionValue("P")));
					LOG.warn("the config file " + file.getName() + " is not find from external,use file in classpath");
				} else {
					FileInputStream fStream = new FileInputStream(file);
					conf.addResource(fStream);
					LOG.info("the config file " + file.getName() + " is load sucess from external");
				}
			}
			if (cmd.hasOption("p")) {
				File file = new File(cmd.getOptionValue("p"));
				if (file.exists()) {
					FileInputStream fStream = new FileInputStream(file);
					conf.addResource(fStream);
					LOG.info("the config file " + file.getName() + " is load sucess from external");
				} else {
					throw new FileNotFoundException("path is:" + file.getPath() + ",file is:" + file.getName());
				}
			}

			conf.setBoolean("mapred.map.tasks.speculative.execution", false);

			props.putAll(cmd.getOptionProperties("D"));

			StringBuilder argsLog = new StringBuilder("Print Configuration [-D key=value]: ");
			for (Object key : props.keySet()) {
				argsLog.append("[" + key.toString() + " = " + props.getProperty(key.toString()) + "] ");
				conf.set(key.toString(), props.getProperty(key.toString()));
			}
			LOG.info(argsLog.toString());

		} catch (ParseException | IOException e) {
			LOG.error("config instince failed", e);
			formatter.printHelp("betl", options);
			System.exit(-1);
		}
		return conf;
	}

}
