package ru.zudin.social;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.ToolRunner;
import ru.zudin.ChainingJob;
import ru.zudin.social.mr.HashMapper;
import ru.zudin.social.mr.UserCollectReducer;
import ru.zudin.social.mr.UserMatchReducer;
import ru.zudin.social.mr.UserPairMapper;

import java.util.Collections;

/**
 * @author sergey
 * @since 30.05.16
 */
public class Executor {

    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        profileMatching(fileSystem);


    }

    private static void profileMatching(FileSystem fileSystem) throws Exception {
        Path outputPath = new Path("output");
        fileSystem.delete(outputPath, true);
        Path tempPath = new Path("temp");
        fileSystem.delete(tempPath, true);

        ChainingJob job = ChainingJob.Builder.instance()
                .name("social_connections")
                .tempDir(tempPath.getName())
//                .mapper(EntityMapper.class)
                .mapper(HashMapper.class)
                .reducer(UserMatchReducer.class)
                .mapper(UserPairMapper.class)
                .reducer(UserCollectReducer.class, Collections.singletonMap("-M", "true"))
                .build();

        ToolRunner.run(new Configuration(), job, new String[]{"input/users.txt", "output"});
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(outputPath, true);
        while (iterator.hasNext()) {
            LocatedFileStatus next = iterator.next();
            Path path = next.getPath();
            String name = path.getName();
            if (!name.endsWith("crc") && !name.equals("_SUCCESS")) {
                String newName = name.replace("-r-00000", "");
                fileSystem.rename(path, new Path("match/" + newName));
            }
        }
        fileSystem.delete(outputPath, true);
        fileSystem.delete(tempPath, true);
    }

}
