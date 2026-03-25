package com.adeptia.ftp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeoutException;

/**
 * FTPTransferPlugin
 * ?????????????????
 * Orchestrates the full FTP-to-FTP file transfer pipeline:
 *
 *   1. Read files from source FTP/SFTP directory
 *   2. Extract GroupId from each filename
 *        Pattern : prefix_GroupId_suffix.<ext>
 *        Example : EENav_101_Transaction.csv -> "101"
 *   3. Lookup TransactionPath from SIC_EENAV.xml mapping
 *        Example : "101"  ?  "EENav_Custom1"
 *   4. Keep original source filename unchanged on target
 *   5. Write file to target FTP under dynamic folder
 *        Example : /target/EENav_Custom1/EENav_101_Transaction.<ext>
 *   6. Delete source file immediately after successful transfer
 *
 * ?? Connection Management ????????????????????????????????????????????????
 *
 *   Uses FTPConnectionManager (not raw FTPFileHandler) which provides:
 *
 *   Auto-reconnect : if connection times out between files,
 *                    transparently gets a new connection and retries
 *   Keep-alive     : sends NOOP every 60s to prevent idle timeout
 *   Retry          : retries failed connections up to 3 times
 *   Health check   : pings connection with NOOP before every file
 *
 * ?? Connection Lifecycle ?????????????????????????????????????????????????
 *
 *   Sequential : one FTPConnectionManager pair for all files
 *                auto-reconnects if timed out between files
 *
 *   Parallel   : each thread has its own FTPConnectionManager pair
 *                each processes one file then disconnects
 *
 * ?? BeanShell Invocation ????????????????????????????????????????????????
 *
 *   import com.adeptia.ftp.AdeptiaFileRouter;
 *
 *   java.util.List results = AdeptiaFileRouter.transfer(
 *       "srcFTPActivityId",
 *       "tgtFTPActivityId",
 *       "/source/",
 *       "/target/",
 *       "/opt/adeptia/ServerKernel/lib/SIC_EENAV.xml",
 *       false,   // parallelMode
 *       3        // maxThreads
 *   );
 */
public class AdeptiaFileRouter {

    private static final Logger LOG = LoggerFactory.getLogger(AdeptiaFileRouter.class);

    private static final int FILE_TRANSFER_TIMEOUT_SECONDS = 120;

    // ?????????????????????????????????????????????????????????????????????
    // TransferResult
    // ?????????????????????????????????????????????????????????????????????

    public static class TransferResult {

        public final String  sourceFile;
        public final String  groupId;
        public final String  transactionPath;
        public final String  targetFileName;
        public final String  targetDir;
        public final boolean success;
        public final boolean deleted;
        public final String  errorMessage;

        public TransferResult(String  sourceFile,
                              String  groupId,
                              String  transactionPath,
                              String  targetFileName,
                              String  targetDir,
                              boolean success,
                              boolean deleted,
                              String  errorMessage) {
            this.sourceFile      = sourceFile;
            this.groupId         = groupId;
            this.transactionPath = transactionPath;
            this.targetFileName  = targetFileName;
            this.targetDir       = targetDir;
            this.success         = success;
            this.deleted         = deleted;
            this.errorMessage    = errorMessage;
        }

        @Override
        public String toString() {
            if (success) {
                return String.format("[OK] %s ? %s/%s (GroupId=%s, Deleted=%s)",
                        sourceFile, targetDir, targetFileName, groupId, deleted);
            } else {
                return String.format("[FAIL] %s ? Error: %s", sourceFile, errorMessage);
            }
        }
    }


    // ?????????????????????????????????????????????????????????????????????
    // Main Entry Point
    // ?????????????????????????????????????????????????????????????????????

    /**
     * @param srcFTPActivityId  Adeptia FTPAccount activity ID for source (FTP/FTPS/SFTP)
     * @param tgtFTPActivityId  Adeptia FTPAccount activity ID for target (FTP/FTPS/SFTP)
     * @param srcPath           Source directory   e.g. "/source/"
     * @param tgtBasePath       Target base dir    e.g. "/target/"
     * @param sicEenavXml       Full path to SIC_EENAV.xml
     * @param parallelMode      false = sequential | true = parallel
     * @param maxThreads        Max concurrent threads (parallel only)
     * @param deleteSourceFile  true  = delete source file after successful transfer (default)
     *                          false = keep source file (copy mode, no deletion)
     * @return                  List<String> each entry is one line of summary/result
     *                              e.g. "TOTAL:150", "SUCCESS:143", "FAILED:7",
     *                              "[OK] file.csv ...", "[FAIL] file.csv error"
     *                              Use context.put() to store these in Adeptia context
     */
    public static List<String> transfer(
            String  srcFTPActivityId,
            String  tgtFTPActivityId,
            String  srcPath,
            String  tgtBasePath,
            String  sicEenavXml,
            boolean parallelMode,
            int     maxThreads,
            boolean deleteSourceFile) throws Exception {

        // Reset counters at start of each batch run
        FTPConnectionManager.resetCounters();

        LOG.info("FTPTransferPlugin started. srcActivity={} tgtActivity={} mode={} deleteSource={}",
                srcFTPActivityId, tgtFTPActivityId,
                parallelMode ? "PARALLEL(maxThreads=" + maxThreads + ")" : "SEQUENTIAL",
                deleteSourceFile);

        // ?? Resolve FTPAccount activities ??????????????????????????????????
        // Both source and target use FTPAccount activity ? works for FTP, FTPS, SFTP
        // Protocol is auto-detected inside FTPAccountConfig based on port + ftpType
        FTPAccountConfig srcCfg = FTPAccountConfig.resolve(srcFTPActivityId, "SOURCE");
        FTPAccountConfig tgtCfg = FTPAccountConfig.resolve(tgtFTPActivityId, "TARGET");

        // ?? Load SIC_EENAV.xml ? read-only, thread-safe ????????????????????
        MutliValueMapLookUp lookup = new MutliValueMapLookUp(sicEenavXml);
        LOG.info("SIC_EENAV mapping loaded from: {}", sicEenavXml);

        // ?? List source files ? dedicated short-lived connection ???????????
        List<String> csvFiles = listSourceFiles(srcCfg, srcPath);

        if (csvFiles.isEmpty()) {
            LOG.warn("No files found in [{}]. Nothing to process.", srcPath);
            List<String> empty = new ArrayList<>();
            empty.add("TOTAL:0");
            empty.add("SUCCESS:0");
            empty.add("FAILED:0");
            empty.add("DELETED:0");
            empty.add("NOT_DELETED:0");
            empty.add("CONNECTIONS_OPENED:0");
            empty.add("RECONNECTS:0");
            return empty;
        }

        LOG.info("Files to process: {} | deleteSource={}", csvFiles.size(), deleteSourceFile);

        // ?? Same-server check ??????????????????????????????????????????????
        boolean sameServer = srcCfg.isSameServer(tgtCfg);
        if (sameServer) {
            LOG.info("Source and target are on the SAME server [{}:{}] protocol={}. " +
                            "Using single shared connection. Expected max connections: 1",
                    srcCfg.host, srcCfg.port, srcCfg.getProtocolName());
        } else if (parallelMode) {
            LOG.info("Source [{}] and target [{}] are DIFFERENT servers. " +
                            "Parallel mode. Expected max connections: {}",
                    srcCfg.host, tgtCfg.host, Math.min(csvFiles.size(), maxThreads) * 2);
        } else {
            LOG.info("Source [{}] protocol={} and target [{}] protocol={} are DIFFERENT servers. " +
                            "Sequential mode. Expected max connections: 2",
                    srcCfg.host, srcCfg.getProtocolName(),
                    tgtCfg.host, tgtCfg.getProtocolName());
        }

        // ?? Process files ??????????????????????????????????????????????????
        List<TransferResult> results;

        if (sameServer) {
            results = transferSingleConnection(srcCfg, srcPath, tgtBasePath, lookup, csvFiles, deleteSourceFile);
        } else if (parallelMode) {
            results = transferParallel(srcCfg, srcPath, tgtCfg, tgtBasePath,
                    lookup, csvFiles, maxThreads, deleteSourceFile);
        } else {
            results = transferSequential(srcCfg, srcPath, tgtCfg, tgtBasePath,
                    lookup, csvFiles, deleteSourceFile);
        }

        // ?? Build String summary list for context.put() ??????????????????
        int total      = results.size();
        int success    = 0;
        int failed     = 0;
        int deleted    = 0;
        int notDeleted = 0;
        for (TransferResult r : results) {
            if  (r.success)              success++;
            else                         failed++;
            if  (r.deleted)              deleted++;
            if  (r.success && !r.deleted) notDeleted++;
        }

        List<String> summary = new ArrayList<>();

        // ?? Count lines ? readable by context.get() in BeanShell ??????????
        summary.add("TOTAL:"            + total);
        summary.add("SUCCESS:"          + success);
        summary.add("FAILED:"           + failed);
        summary.add("DELETED:"          + deleted);
        summary.add("NOT_DELETED:"      + notDeleted);
        summary.add("CONNECTIONS_OPENED:" + FTPConnectionManager.getTotalConnectionsOpened());
        summary.add("RECONNECTS:"        + FTPConnectionManager.getTotalReconnects());

        // ?? Per-file result lines ??????????????????????????????????????????
        for (TransferResult r : results) {
            summary.add(r.toString());
        }

        LOG.info("FTPTransferPlugin completed. TOTAL={} SUCCESS={} FAILED={} DELETED={} NOT_DELETED={}",
                total, success, failed, deleted, notDeleted);
        LOG.info("Final connection stats ? {}", FTPConnectionManager.getConnectionStats());

        return summary;
    }

    // ?????????????????????????????????????????????????????????????????????
    // Sequential Processing
    // ?????????????????????????????????????????????????????????????????????

    /**
     * One FTPConnectionManager pair shared across all files.
     *
     * Connection behaviour:
     *   - getConnection() health-checks before every file
     *   - If timed out ? auto-reconnects transparently
     *   - Keep-alive NOOP sent every 60s between files
     *   - Both managers closed in try-with-resources on exit
     *
     * Per-file order: read ? write ? completePending ? delete ? next file
     */
    private static List<TransferResult> transferSequential(
            FTPAccountConfig srcCfg, String srcPath,
            FTPAccountConfig tgtCfg, String tgtBasePath,
            MutliValueMapLookUp lookup, List<String> files,
            boolean deleteSourceFile) {

        List<TransferResult> results = new ArrayList<>();

        try (FTPConnectionManager srcMgr = new FTPConnectionManager(srcCfg, "SOURCE");
             FTPConnectionManager tgtMgr = new FTPConnectionManager(tgtCfg, "TARGET")) {

            srcMgr.connect();
            tgtMgr.connect();

            int index = 1;
            for (String fileName : files) {
                LOG.info("[{}/{}] {}", index, files.size(), fileName);
                TransferResult result;
                try {
                    // getConnection() checks health and reconnects if timed out
                    IFileHandler src = srcMgr.getConnection();
                    IFileHandler tgt = tgtMgr.getConnection();
                    result = processFile(src, tgt, lookup, srcPath, tgtBasePath, fileName, deleteSourceFile);
                } catch (Exception e) {
                    LOG.error("Failed to process file [{}]: {}", fileName, e.getMessage(), e);
                    result = new TransferResult(
                            fileName, "", "", "", "", false, false, e.getMessage());
                }
                results.add(result);
                index++;
            }

        } catch (IOException e) {
            LOG.error("FTP connection-level failure in sequential mode: {}", e.getMessage(), e);
        }
        // Both source and target managers guaranteed closed here

        return results;
    }

    // ?????????????????????????????????????????????????????????????????????
    // Parallel Processing
    // ?????????????????????????????????????????????????????????????????????

    /**
     * Each thread has its own FTPConnectionManager pair.
     * Each thread processes one file then its managers are closed.
     *
     * Connection behaviour per thread:
     *   - connect ? health-check ? process one file ? disconnect
     *   - If connection fails ? retried up to 3 times before failing
     *   - try-with-resources guarantees close even on exception
     */
    private static List<TransferResult> transferParallel(
            FTPAccountConfig srcCfg, String srcPath,
            FTPAccountConfig tgtCfg, String tgtBasePath,
            MutliValueMapLookUp lookup, List<String> files,
            int maxThreads, boolean deleteSourceFile) {

        int threadCount = Math.min(files.size(), maxThreads);
        LOG.info("Starting parallel transfer. threadPoolSize={}", threadCount);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<TransferResult>> futures = new ArrayList<>();

        for (String fileName : files) {
            futures.add(executor.submit(() -> {
                LOG.info("Thread [{}] processing: {}",
                        Thread.currentThread().getName(), fileName);

                // Each thread: own FTPConnectionManager pair, closed on exit
                try (FTPConnectionManager srcMgr = new FTPConnectionManager(srcCfg, "SOURCE");
                     FTPConnectionManager tgtMgr = new FTPConnectionManager(tgtCfg, "TARGET")) {

                    srcMgr.connect();
                    tgtMgr.connect();

                    IFileHandler src = srcMgr.getConnection();
                    IFileHandler tgt = tgtMgr.getConnection();

                    return processFile(src, tgt, lookup, srcPath, tgtBasePath, fileName, deleteSourceFile);

                } catch (Exception e) {
                    LOG.error("Thread [{}] failed for [{}]: {}",
                            Thread.currentThread().getName(), fileName, e.getMessage(), e);
                    return new TransferResult(
                            fileName, "", "", "", "", false, false, e.getMessage());
                }
                // srcMgr and tgtMgr guaranteed closed here
            }));
        }

        // Collect results
        List<TransferResult> results = new ArrayList<>();
        for (Future<TransferResult> future : futures) {
            try {
                results.add(future.get(FILE_TRANSFER_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            } catch (TimeoutException e) {
                LOG.error("Transfer timed out after {}s.", FILE_TRANSFER_TIMEOUT_SECONDS);
                results.add(new TransferResult(
                        "unknown", "", "", "", "", false, false, "Transfer timeout"));
            } catch (ExecutionException e) {
                LOG.error("Thread execution error: {}", e.getCause().getMessage(), e.getCause());
                results.add(new TransferResult(
                        "unknown", "", "", "", "", false, false, e.getCause().getMessage()));
            } catch (InterruptedException e) {
                LOG.warn("Thread interrupted.");
                Thread.currentThread().interrupt();
            }
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                LOG.warn("Thread pool did not terminate in 60s. Forcing shutdown.");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        return results;
    }

    // ?????????????????????????????????????????????????????????????????????
    // Core Per-File Logic
    // ?????????????????????????????????????????????????????????????????????

    /**
     * Processes a single CSV file:
     *
     *   EENav_101_Transaction.<ext>
     *       ?  GroupIdExtractor  ?  "101"
     *       ?  SICEENAVLookup   ?  "EENav_Custom1"
     *       ?  readFile()       ?  DATA channel open, CONTROL locked
     *       ?  writeFile()      ?  stream to target (deletes existing first)
     *       ?  completePending  ?  CONTROL released  [in finally]
     *       ?  deleteFile()     ?  DELE source       [only if write succeeded]
     *
     * Note: source and target here are obtained via FTPConnectionManager.getConnection()
     *       which has already verified the connection is alive before returning.
     */
    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmssSSS");

    private static TransferResult processFile(
            IFileHandler source,
            IFileHandler target,
            MutliValueMapLookUp lookup,
            String srcPath,
            String tgtBasePath,
            String fileName,
            boolean deleteSourceFile) throws IOException {

        // 1. Extract GroupId and lookup TransactionPath
        String groupId         = GroupIdExtractor.extract(fileName);
        String transactionPath = lookup.lookup(groupId, "TransactionPath");
        String srcFullPath     = normalizePath(srcPath) + fileName;
        String targetDir       = normalizePath(tgtBasePath) + transactionPath;

        // 2. Resolve target filename
        //    If a file with the same name already exists at target,
        //    append a timestamp to avoid overwriting:
        //      original  : report.csv
        //      conflict  : report_20240323_143022123.csv
        String targetFileName = resolveTargetFileName(target, targetDir, fileName);

        LOG.info("[TRANSFER] {} -> {}/{}", fileName, targetDir, targetFileName);

        // 3. Read source file into memory buffer
        //    readFile() buffers the entire file AND calls completePendingCommand()
        //    internally for FTP, so the control channel is free before writeFile().
        InputStream fileStream;
        try {
            fileStream = source.readFile(srcFullPath);
        } catch (IOException e) {
            LOG.error("[FAILED] Read error for [{}]: {}", fileName, e.getMessage());
            // Attempt to release control channel on read failure
            try { source.completePendingCommand(); } catch (IOException ignored) {}
            return new TransferResult(
                    fileName, groupId, transactionPath, targetFileName, targetDir,
                    false, false, "Read failed: " + e.getMessage());
        }
        // Note: completePendingCommand() is called inside readFile() on success path.

        // 4. Write to target
        boolean success;
        try {
            success = target.writeFile(targetDir, targetFileName, fileStream);
        } catch (IOException e) {
            LOG.error("[FAILED] Write error for [{}]: {}", fileName, e.getMessage());
            return new TransferResult(
                    fileName, groupId, transactionPath, targetFileName, targetDir,
                    false, false, "Write failed: " + e.getMessage());
        }

        if (!success) {
            LOG.error("[FAILED] Write returned false for [{}] -> [{}/{}]",
                    fileName, targetDir, targetFileName);
            return new TransferResult(
                    fileName, groupId, transactionPath, targetFileName, targetDir,
                    false, false, "Write returned false");
        }

        // 5. Delete source file ? only if deleteSourceFile=true
        boolean deleted = false;
        if (deleteSourceFile) {
            deleted = source.deleteFile(srcFullPath);
            if (!deleted) {
                LOG.error("[NOT DELETED] Source [{}] could not be deleted. " +
                                "File transferred successfully but manual cleanup required.",
                        srcFullPath);
            }
        } else {
            LOG.info("[KEPT] Source file [{}] kept (deleteSourceFile=false).", srcFullPath);
        }

        LOG.info("[OK] {} -> {}/{} | deleted={}", fileName, targetDir, targetFileName, deleted);

        return new TransferResult(
                fileName, groupId, transactionPath, targetFileName, targetDir,
                true, deleted, null);
    }

    /**
     * Resolves the target filename.
     * If a file with the same name already exists at target, appends a
     * millisecond-precision timestamp to avoid overwriting.
     *
     * Uses a targeted single-file listing instead of listing the entire directory
     * to avoid N extra round-trips for N files in large batches.
     *
     * Examples:
     *   report.csv       (no conflict)  -> report.csv
     *   report.csv       (exists)       -> report_20240323_143022456.csv
     *   data.txt         (exists)       -> data_20240323_143022456.txt
     *   archive.tar.gz   (exists)       -> archive.tar_20240323_143022456.gz
     *   noextension      (exists)       -> noextension_20240323_143022456
     */
    private static String resolveTargetFileName(
            IFileHandler target, String targetDir, String originalName) {

        // Check existence by listing only the specific file path.
        // listFiles(dir/file) returns the single entry if the file exists,
        // empty array if it does not -- avoids listing the full directory.
        String fullTargetPath = normalizePath(targetDir) + originalName;
        try {
            IFileHandler.FileEntry[] entries = target.listFiles(fullTargetPath);
            if (entries != null && entries.length > 0) {
                // File exists -- append timestamp
                String timestamp = LocalDateTime.now().format(TS_FMT);
                String newName   = appendTimestamp(originalName, timestamp);
                LOG.warn("[CONFLICT] [{}] exists at [{}]. Renaming to [{}].",
                        originalName, targetDir, newName);
                return newName;
            }
        } catch (Exception e) {
            // Listing failed -- directory likely does not exist yet.
            // No conflict possible; return original name.
        }

        return originalName;
    }

    /**
     * Inserts a timestamp before the last file extension.
     *   report.csv        + 20240323_143022456  ->  report_20240323_143022456.csv
     *   data.tar.gz       + 20240323_143022456  ->  data.tar_20240323_143022456.gz
     *   noextension       + 20240323_143022456  ->  noextension_20240323_143022456
     */
    private static String appendTimestamp(String fileName, String timestamp) {
        int dot = fileName.lastIndexOf('.');
        if (dot > 0) {
            return fileName.substring(0, dot) + "_" + timestamp + fileName.substring(dot);
        }
        return fileName + "_" + timestamp;
    }

    // ?????????????????????????????????????????????????????????????????????
    // Helper ? List Source Files
    // ?????????????????????????????????????????????????????????????????????

    /**
     * Lists all transferable files using a dedicated short-lived connection.
     * try-with-resources guarantees the listing connection is always closed.
     */
    private static List<String> listSourceFiles(
            FTPAccountConfig cfg, String path) throws Exception {



        try (FTPConnectionManager listMgr = new FTPConnectionManager(cfg, "LIST")) {

            listMgr.connect();
            IFileHandler.FileEntry[] files = listMgr.getConnection().listFiles(path);

            LOG.info("Total entries in source [{}]: {}", path, files.length);
            return filterFiles(files, path);
        }
    }


    // ?????????????????????????????????????????????????????????????????????
    // Single-Connection Processing (same source + target server)
    // ?????????????????????????????????????????????????????????????????????

    /**
     * Processes all files using ONE FTPConnectionManager (one FTP connection).
     * Used when source and target are on the same FTP server.
     *
     * The same FTPFileHandler is passed as both source and target to
     * processFile(). This works because:
     *   - readFile()  opens a DATA channel for RETR
     *   - After completePendingCommand() that DATA channel closes
     *   - writeFile() then opens a NEW DATA channel for STOR
     *   - Both use the same CONTROL channel sequentially ? no conflict
     *   - deleteFile() uses the same CONTROL channel after write completes
     *
     * Connection behaviour:
     *   - One FTPConnectionManager, one login, one keep-alive NOOP stream
     *   - getConnection() health-checks and auto-reconnects if timed out
     *   - try-with-resources guarantees disconnect on block exit
     *
     * Per-file order: read ? write ? completePending ? delete ? next file
     */
    private static List<TransferResult> transferSingleConnection(
            FTPAccountConfig cfg, String srcPath, String tgtBasePath,
            MutliValueMapLookUp lookup, List<String> files,
            boolean deleteSourceFile) {

        List<TransferResult> results = new ArrayList<>();

        // Single FTPConnectionManager for both read and write ? same server
        try (FTPConnectionManager mgr = new FTPConnectionManager(cfg, "SINGLE")) {

            mgr.connect();

            int index = 1;
            for (String fileName : files) {
                LOG.info("[{}/{}] {}", index, files.size(), fileName);
                TransferResult result;
                try {
                    // Same handler used for both source and target operations
                    IFileHandler handler = mgr.getConnection();
                    result = processFile(handler, handler, lookup,
                            srcPath, tgtBasePath, fileName, deleteSourceFile);
                } catch (Exception e) {
                    LOG.error("Failed to process file [{}]: {}", fileName, e.getMessage(), e);
                    result = new TransferResult(
                            fileName, "", "", "", "", false, false, e.getMessage());
                }
                results.add(result);
                index++;
            }

        } catch (IOException e) {
            LOG.error("FTP connection failure in single-connection mode: {}", e.getMessage(), e);
        }
        // Connection guaranteed closed here

        return results;
    }

    // ?????????????????????????????????????????????????????????????????????
    // Helper ? Filter CSV Files
    // ?????????????????????????????????????????????????????????????????????

    /**
     * Filters remote file entries for transfer.
     * Accepts ALL file types -- skips only directories and hidden files (dot-prefix).
     */
    private static List<String> filterFiles(IFileHandler.FileEntry[] files, String srcPath) {
        List<String> result = new ArrayList<>();
        if (files == null) return result;

        for (IFileHandler.FileEntry file : files) {
            // Skip directories and symlinks -- only transfer regular files
            if (!file.isFile) continue;

            String name = file.name;

            // Skip blank names (defensive)
            if (name == null || name.trim().isEmpty()) continue;

            // Skip hidden files (dot-prefix)
            if (name.startsWith(".")) continue;

            LOG.info("Queued: [{}] ({} bytes)", name, file.size);
            result.add(name);
        }

        if (result.isEmpty()) {
            LOG.warn("No transferable files found in [{}].", srcPath);
        }
        return result;
    }

    // ?????????????????????????????????????????????????????????????????????
    // Helper ? Path Normalization
    // ?????????????????????????????????????????????????????????????????????

    private static String normalizePath(String path) {
        if (path == null || path.isEmpty()) return "/";
        return path.endsWith("/") ? path : path + "/";
    }
}