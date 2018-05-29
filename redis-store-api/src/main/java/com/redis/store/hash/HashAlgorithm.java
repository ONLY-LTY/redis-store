package com.redis.store.hash;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.CRC32;

/**
 * Known hashing algorithms for locating a server for a key. Note that all hash
 * algorithms return 64-bits of hash, but only the lower 32-bits are
 * significant. This allows a positive 32-bit number to be returned for all
 * cases.
 */
public enum HashAlgorithm {

    /**
     * Native hash (String.hashCode()).
     */
    NATIVE_HASH,
    /**
     * CRC32_HASH as used by the perl API. This will be more consistent both
     * across multiple API users as well as java versions, but is mostly likely
     * significantly slower.
     */
    CRC32_HASH,
    /**
     * MD5-based hash algorithm used by ketama.
     */
    KETAMA_HASH,

    /** 11 % 10 = 1 */
    MOD,

    /** A % 10 = 0 */
    HEX_PREFIX_MOD;

    public static String CHARSET = "utf-8";

    /**
     * Compute the hash for the given key.
     * 
     * @return a positive integer hash
     */
    public long hash(final String k) {
        long rv = 0;
        switch (this) {
        case NATIVE_HASH:
            rv = k.hashCode();
            break;
        case CRC32_HASH:
            // return (crc32(shift) >> 16) & 0x7fff;
            CRC32 crc32 = new CRC32();
            crc32.update(getBytes(k));
            rv = crc32.getValue() >> 16 & 0x7fff;
            break;
        case KETAMA_HASH:
            byte[] bKey = computeMd5(k);
            rv = (long) (bKey[3] & 0xFF) << 24 | (long) (bKey[2] & 0xFF) << 16 | (long) (bKey[1] & 0xFF) << 8 | bKey[0] & 0xFF;
            break;
        default:
            assert false;
        }

        return rv & 0xffffffffL; /* Truncate to 32-bits */
    }

    public long hash(byte[] bytes) {
        long rv = 0;
        switch (this) {
            case NATIVE_HASH:
                if (bytes.length > 0) {
                    for (byte b : bytes) {
                        rv = 31 * rv + b;
                    }
                }
                break;
            case CRC32_HASH:
                CRC32 crc32 = new CRC32();
                crc32.update(bytes);
                rv = crc32.getValue() >> 16 & 0x7fff;
                break;
            case KETAMA_HASH:
                byte[] bytesMd5 = computeMd5(bytes);
                rv = (long) (bytesMd5[3] & 0xFF) << 24 |
                        (long) (bytesMd5[2] & 0xFF) << 16 |
                        (long) (bytesMd5[1] & 0xFF) << 8 |
                        bytesMd5[0] & 0xFF;
                break;
            default:
                throw new Error();
        }
        return rv & 0xffffffffL; /* Truncate to 32-bits */
    }

    private static ThreadLocal<MessageDigest> md5Local = new ThreadLocal<MessageDigest>();

    /**
     * Get the md5 of the given key.
     */
    public static byte[] computeMd5(String string) {
        return computeMd5(getBytes(string));
    }

    public static byte[] computeMd5(byte[] bytes) {
        MessageDigest md5 = md5Local.get();
        if (md5 == null) {
            try {
                md5 = MessageDigest.getInstance("MD5");
                md5Local.set(md5);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("MD5 not supported", e);
            }
        }
        md5.reset();
        md5.update(bytes);
        return md5.digest();
    }

    private static final byte[] getBytes(String k) {
        if (k == null || k.length() == 0) {
            throw new IllegalArgumentException("Key must not be blank");
        }
        try {
            return k.getBytes(CHARSET);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

}
