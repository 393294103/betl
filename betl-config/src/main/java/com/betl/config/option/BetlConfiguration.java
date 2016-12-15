/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月15日下午12:00:51
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.config.option;

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

/**
 * @author zhl
 *
 */
public class BetlConfiguration {
	// 2. 声明一个Logger，这个是static的方式，我比较习惯这么写。
	private Logger logger = LoggerFactory.getLogger(getClass());

	
	private  Options buildOptions() {
		Options options = new Options();
		// 外部传入的公共文件
		Option P = OptionBuilder.withArgName("x").hasArgs().withDescription("external public XML filename.").create("x");
		P.setRequired(true);
		options.addOption(P);
		// 外部传入的私有文件
		Option p = OptionBuilder.withArgName("p").hasArgs().withDescription("external detail properties filename.").create("p");
		P.setRequired(true);
		options.addOption(p);
		// 终极key=value覆盖
		options.addOption(OptionBuilder.withArgName("property=value").hasArgs(2).withValueSeparator().withDescription("use value for given property.").create("D"));
		return options;
	}

	public Configuration getConfiguration(String[] args) throws IOException {
		
		Options options = buildOptions();
		Configuration conf = new Configuration();

		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(200);

		CommandLineParser parser=null;
		CommandLine cmd=null;
		try {
			 parser = new PosixParser();
			 cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.out.println("[error]\t"+e.getMessage());
			formatter.printHelp("betl", options);
			System.exit(-1);
		}
		
		if (cmd.hasOption("x")) {
			String xmlPath = cmd.getOptionValue("x");
			ICompentConfiguration icf = new XmlConfigruation(xmlPath);
			conf.addResource(icf.getConfiguration());
			logger.debug("[getConfiguration]加载xml文件{}",xmlPath);
		}
		
		if (cmd.hasOption("p")) {
			String propsPath = cmd.getOptionValue("p");
			ICompentConfiguration icf = new PropsConfigruation(propsPath);
			conf.addResource(icf.getConfiguration());
			logger.info("[getConfiguration]加载properties文件{}",propsPath);
		}

		if (cmd.hasOption("D")) {
			Properties props = cmd.getOptionProperties("D");

			ICompentConfiguration icf = new PropsConfigruation(props);
			conf.addResource(icf.getConfiguration());
			logger.info("[getConfiguration]加载输入参数-D{}",props.toString());
		}
		return conf;

	}

	
	public static void main(String[] args) throws ParseException, IOException {
		BetlConfiguration bconf=new BetlConfiguration();
		System.out.println(bconf.getConfiguration(args));
		System.out.println(bconf.getConfiguration(args).get("log4j.rootLogger"));
		System.out.println(bconf.getConfiguration(args).get("mysql.jdbc.username"));
		System.out.println(bconf.getConfiguration(args).get("abc"));
	}
	
}
