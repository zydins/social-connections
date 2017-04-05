package ru.zudin.social;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import ru.zudin.ChainingJob;
import ru.zudin.social.mr.HashMapper;
import ru.zudin.social.mr.UserMatchReducer;

/**
 * @author sergey
 * @since 30.05.16
 */
public class Executor {

    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        fileSystem.delete(new Path("output"), true);
        fileSystem.delete(new Path("temp1"), true);

        ChainingJob job = ChainingJob.Builder.instance()
                .name("social_connections")
                .tempDir("temp")
//                .mapper(EntityMapper.class)
                .mapper(HashMapper.class)
                .reducer(UserMatchReducer.class)
                .build();

        ToolRunner.run(new Configuration(), job, new String[]{"input/second.txt", "output"});



//        Preconditions.checkArgument(args.length >= 1);
//        Configuration configuration = new Configuration();
//        Job job = Job.getInstance(configuration, "alertbutton");
//        job.setJarByClass(Executor.class);
//        Class<DeviceMapper> mapperClass = DeviceMapper.class;
//        Class<GeoReducer> reducerClass = GeoReducer.class;
//        job.setMapperClass(mapperClass);
//        Class<?>[] typeArgs = TypeResolver.resolveRawArguments(Mapper.class, mapperClass);
//        job.setMapOutputKeyClass(typeArgs[2]);
//        job.setMapOutputValueClass(typeArgs[3]);
//        typeArgs = TypeResolver.resolveRawArguments(Reducer.class, reducerClass);
//        job.setOutputKeyClass(typeArgs[2]);
//        job.setOutputValueClass(typeArgs[3]);
//        job.setReducerClass(reducerClass);
//
//        FileSystem fileSystem = FileSystem.get(new Configuration());
//        Path input = new Path(args[0]);
//        Path output = new Path("output");
//        fileSystem.delete(output, true);
//        FileInputFormat.addInputPath(job, input);
//        FileOutputFormat.setOutputPath(job, output);
//
//        if (!job.waitForCompletion(true)) {
//            System.out.println("Cannot complete job");
//        }
//        System.out.println("Done");

    }

}
