package ru.zudin.social;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.ToolRunner;
import ru.zudin.ChainingJob;
import ru.zudin.social.mr.matcher.HashMapper;
import ru.zudin.social.mr.matcher.UserCollectReducer;
import ru.zudin.social.mr.matcher.UserMatchReducer;
import ru.zudin.social.mr.matcher.UserPairMapper;

import java.util.Collections;

/**
 * @author sergey
 * @since 30.05.16
 */
public class Executor {

    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        profileMatching(fileSystem);
//        friendMatching(fileSystem);

    }

    private static void friendMatching(FileSystem fileSystem) throws Exception {
        Path outputPath = new Path("friends");
        Path tempPath = new Path("temp");
        fileSystem.delete(outputPath, true);
        fileSystem.delete(tempPath, true);
        RemoteIterator<FileStatus> iterator2 = fileSystem.listStatusIterator(outputPath.getParent());
        while (iterator2.hasNext()) {
            FileStatus next = iterator2.next();
            if (next.isDirectory() && next.getPath().getName().startsWith("temp")) {
                fileSystem.delete(next.getPath(), true);
            }
        }

//        ChainingJob job = ChainingJob.Builder.instance()
//                .name("social_connections_2")
//                .tempDir(tempPath.getName())
//                .mapper(HashMapper.class)
//                .reducer(UserMatchReducer.class)
//                .mapper(UserPairMapper.class)
//                .reducer(UserCollectReducer.class, Collections.singletonMap("-M", "true"))
//                .build();
//
//        ToolRunner.run(new Configuration(), job, new String[]{"match", "friends"});
    }

    private static void profileMatching(FileSystem fileSystem) throws Exception {
        Path outputPath = new Path("output");
        Path tempPath = new Path("temp");
        fileSystem.delete(outputPath, true);
        fileSystem.delete(tempPath, true);
        Path matchPath = new Path("match");
        fileSystem.delete(matchPath, true);
        RemoteIterator<FileStatus> iterator2 = fileSystem.listStatusIterator(outputPath.getParent());
        while (iterator2.hasNext()) {
            FileStatus next = iterator2.next();
            if (next.isDirectory() && next.getPath().getName().startsWith("temp")) {
                fileSystem.delete(next.getPath(), true);
            }
        }

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
    }

}
