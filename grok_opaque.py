#!/usr/bin/python

"""Dump chunk headers from a saved opaque data file."""

# Put COPYRIGHT_NOTICE here, *after* the module's docstring.

import argparse
import errno
import os
import struct
import sys
import textwrap
import traceback

from collections import namedtuple
from contextlib import contextmanager
from scidblib import statistics

_args = None                    # Parsed arguments
_pgm = None                     # Program name

PAYLOAD_MAGIC = 0xDDDDAAAA000EAAAC
EBM_MAGIC     = 0xEEEEAAAA00EEBAAC


class AppError(Exception):
    """Base class for all exceptions that halt script execution."""
    pass


class BadMagicError(Exception):
    """Bad magic number, we're lost in a maze of structs all different."""
    pass


def roundup(x, y):
    """Round x up to the next multiple of y."""
    return ((x+y-1) // y) * y


def dbg(*args):
    if _args.verbose:
        print >>sys.stderr, _pgm, ' '.join(str(x) for x in args)


def warn(*args):
    sys.stdout.flush()
    print >>sys.stderr, "Warning:", ' '.join(str(x) for x in args)


def problem(*args):
    print "Problem:", ' '.join(str(x) for x in args)


def safe_mean(x):
    return statistics.mean(x) if x else "(no data)"


def safe_stdev(x):
    if not x:
        return "(no data)"
    if len(x) == 1:
        return "(one datum: {0})".format(x[0])
    return statistics.stdev(x)


@contextmanager
def save_excursion(fh):
    """Save current file offset, restore it after with-statement body."""
    saved_offset = fh.tell()
    yield
    fh.seek(saved_offset, os.SEEK_SET)


class StructParser(object):

    """Generic parsing machinery customizable for various structures."""

    type_id = 0

    def __init__(self, format=None, size=None, attrs=None, magic=None,
                 type_name=None, as_hex=None):
        assert format is not None, "format required"
        assert size is not None, "size required"
        assert attrs is not None, "attrs required"
        self.format = format
        self.size = size
        self.magic = magic
        if as_hex is None:
            self.as_hex = []
        elif isinstance(as_hex, basestring):
            self.as_hex = as_hex.split()
        else:
            self.as_hex = as_hex
        self.result = None
        if isinstance(attrs, basestring):
            attrs = attrs.split()
        if not type_name:
            type_name = "SP%d" % StructParser.type_id
            StructParser.type_id += 1
        self.type_name = type_name
        self.type = namedtuple(type_name, attrs)
        # Test consistency of size and format.
        try:
            dummy = '*' * size
            struct.unpack(self.format, dummy)
        except struct.error as e:
            raise AppError("Format '{0}' and size {1} disagree: {2}".format(
                    self.format, self.size, e))

    def parse(self, data):
        """Return parsed tuple tuple for given data."""
        assert len(data) == self.size, "Wrong data length"
        self.result = self.type._make(struct.unpack(self.format, data))
        if self.magic:
            try:
                if self.result.magic != self.magic:
                    raise BadMagicError(' '.join((
                                self.type_name, "magic number mismatch:",
                                hex(self.result.magic), "should be",
                                hex(self.magic))))
            except AttributeError:
                # No magic attribute in result, so no check needed.
                pass
        return self.result

    def to_str(self, tup=None):
        t = self.result if tup is None else tup
        def hexify(field, value):
            return hex(int(value)) if field in self.as_hex else str(value)
        s = '\n'.join(textwrap.wrap(
                ' '.join(':'.join((f, hexify(f, v)))
                         for f, v in zip(self.type._fields, t))))
        return s


class OpaqueChunkHeader(StructParser):

    """
    See src/smgr/io/TemplateParser.h :

        uint32_t magic;
        uint32_t version;
        uint32_t size;
        uint32_t signature;
        uint64_t attrId;
        int8_t   compressionMethod;
        uint8_t  flags;
        uint8_t  nDims;
    """

    ARRAY_FLAG = 8
    RLE_FLAG = 2

    def __init__(self):
        StructParser.__init__(
            self,
            format="<4LQbBB5x",
            size=roundup(27, 8),
            magic=0x5AC00E,
            attrs="magic version size sig attr comp flags ndims",
            as_hex="magic sig flags",
            type_name="OpaqueChunkHdr")

    def parse(self, data):
        """Additional error checking on parse."""
        StructParser.parse(self, data)
        if (self.result.flags != self.ARRAY_FLAG and
            self.result.flags != self.RLE_FLAG):
            raise AppError("Unknown OpaqueChunkHeader flags: {0}\n{1}".format(
                    hex(self.result.flags), self.result))
        return self.result

    def flags(self):
        """Pretty-print flags from last parsed header."""
        assert self.result is not None, "Early flags() call"
        if self.result.flags == 2:
            return 'rle'
        elif self.result.flags == 8:
            return 'array'
        return hex(self.result.flags)


class RleHeader(StructParser):

    """Parser for ConstRLEPayload::Header"""


    def __init__(self, data=None):
        StructParser.__init__(
            self,
            format="<5QB7x",
            size=roundup(41, 8),
            magic=PAYLOAD_MAGIC,
            attrs="magic nsegs elem_size data_size var_offs is_bool",
            as_hex="magic",
            type_name="RleHdr")
                  

class RleSegmentParser(StructParser):

    """Parse an rle::Segment structure."""

    def __init__(self):
        StructParser.__init__(
            self,
            format="<QI",
            size=12,            # already packed, no need to roundup
            attrs="start allbits",
            type_name="RleSeg")

    def data_index(self, tup=None):
        t = self.result if tup is None else tup
        return t.allbits & 0x3FFFFFFF  # 30 bits

    def is_run(self, tup=None):
        t = self.result if tup is None else tup
        return 1 if t.allbits & 0x40000000 else 0

    def is_null(self, tup=None):
        t = self.result if tup is None else tup
        return 1 if t.allbits & 0x80000000 else 0

    def to_str(self, tup=None):
        """Special to_str() to unpack bitfields."""
        t = self.result if tup is None else tup
        return "start:{0} index:{1} run:{2} null:{3}".format(
            t.start, self.data_index(t), self.is_run(t), self.is_null(t))


class EbmHeader(StructParser):

    """Parse an empty bitmap header."""

    def __init__(self):
        StructParser.__init__(
            self,
            format="<3Q",
            size=24,
            magic=EBM_MAGIC,
            attrs="magic nsegs nelems",
            as_hex="magic",
            type_name="EbmHdr")


class EbmSegment(StructParser):

    """Parse an empty bitmap segment."""

    def __init__(self):
        StructParser.__init__(
            self,
            format="<3q",
            size=24,
            attrs="lpos length ppos",
            type_name="EbmSeg")


def dump_rle_segments(fh, rlehdr):
    """Print segment data found at current file offset."""
    seg_parser = RleSegmentParser()
    nsegs = rlehdr.nsegs
    nruns = 0
    run_counts = []
    est_data_size = 0
    prev_seg = seg_parser.type._make((0, 0))
    for segnum in xrange(nsegs + 1):  # +1 to include final guardian segment
        # Read and parse rle::Segment struct
        raw_bytes = fh.read(seg_parser.size)
        assert len(raw_bytes) == seg_parser.size, (
            "Expecting segment")
        seg = seg_parser.parse(raw_bytes)
        # Gather run-length data and maintain estimated data size.
        prev_run = seg_parser.is_run(prev_seg)
        if prev_run:
            runlen = seg.start - prev_seg.start
            run_counts.append(runlen)
            nruns += 1
            est_data_size += rlehdr.elem_size
        else:
            est_data_size += rlehdr.elem_size * (seg.start - prev_seg.start)
        # Print segment.
        print "{{seg:{0} {1}}}{2}".format(
            segnum, seg_parser.to_str(seg),
            " {prev_run:%d}" % runlen if prev_run else "")
        prev_seg = seg
    # Print run statistics.
    print "Run stats: segs={0} runs={1} mean={2} stdev={3}".format(
        nsegs, nruns, safe_mean(run_counts),
        safe_stdev(run_counts))
    print "Data size, estimated:", est_data_size, "Actual:", rlehdr.data_size
    if est_data_size != rlehdr.data_size:
        problem("DATA SIZE MISMATCH, est {0} != actual {1}".format(
                est_data_size, rlehdr.data_size))


def dump_rle_payload(fh):
    """Print RLE payload at current file offset, returning the header."""
    starting_offset = fh.tell()
    rle_parser = RleHeader()
    raw_bytes = fh.read(rle_parser.size)
    assert len(raw_bytes) == rle_parser.size, "Expecting RLE payload"
    try:
        rlehdr = rle_parser.parse(raw_bytes)
    except BadMagicError:
        # Get back to where we once belonged!
        fh.seek(starting_offset, os.SEEK_SET)
        raise
    print "-- RLE Header:"
    print rle_parser.to_str(rlehdr)
    if rlehdr.var_offs:
        warn("This script has not (yet) been tested for chunks"
             " of variable length attributes")
    else:
        dump_rle_segments(fh, rlehdr)
    return rlehdr


def summarize_ebm_segments(fh, ebmhdr):
    """Summarize the segments, because printing them all is too boring."""
    seg_parser = EbmSegment()
    nsegs = ebmhdr.nsegs
    lengths = []
    for segnum in xrange(ebmhdr.nsegs):
        # Read and parse ConstRLEEmptyBitmap::Segment
        raw_bytes = fh.read(seg_parser.size)
        assert len(raw_bytes) == seg_parser.size, "Expecting EBM segment"
        seg = seg_parser.parse(raw_bytes)
        # Collect the segment lengths, the only thing we really care about.
        lengths.append(seg.length)
    # Print statistics on runs of contiguous cells.
    print "EBM stats: segs={0} mean={1} stdev={2}".format(
        ebmhdr.nsegs, safe_mean(lengths), safe_stdev(lengths))
    nonempty_count = sum(lengths)
    if ebmhdr.nelems != nonempty_count:
        problem("Expected", ebmhdr.nelems, "non-empty cells but counted only",
                nonempty_count, "cells")


def dump_empty_bitmap(fh):
    """Print summary of EBM payload at current file offset."""
    ebm_parser = EbmHeader()
    raw_bytes = fh.read(ebm_parser.size)
    assert len(raw_bytes) == ebm_parser.size, "Expecting EBM payload"
    ebmhdr = ebm_parser.parse(raw_bytes)
    print "-- EBM Header:"
    print ebm_parser.to_str(ebmhdr)
    summarize_ebm_segments(fh, ebmhdr)
    return ebmhdr


def process_one_file(fh, file_name):
    """Process an open input file."""
    # Get parsers we'll need.
    och_parser = OpaqueChunkHeader()
    gaps = 0
    while True:
        offset = fh.tell()
        dbg("Read next OCH in", file_name, "at offset", offset)
        hdr_bytes = fh.read(och_parser.size)
        if not hdr_bytes:
            if gaps:
                problem(gaps, "bytes unaccounted for!")
            break;              # EOF
        if len(hdr_bytes) < och_parser.size:
            warn("Short header found at", offset, "in", file_name, "(quitting)")
            return
        hdr = och_parser.parse(hdr_bytes)
        dbg("Read OCH:", hdr)

        # For RLE chunks, the chunk position is not included in hdr.size.
        is_array = (hdr.flags == OpaqueChunkHeader.ARRAY_FLAG)
        if is_array:
            chunk_pos = None
        else:
            chunk_pos = struct.unpack("<%dq" % hdr.ndims,
                                      fh.read(8 * hdr.ndims))
           
        print "== Chunk at", ':'.join((file_name, str(offset))), chunk_pos
        print och_parser.to_str(hdr)

        expected_next_chunk = None
        rlehdr = None
        if not is_array:
            dbg("Read RLE header at offset", fh.tell())
            with save_excursion(fh):
                try:
                    rlehdr = dump_rle_payload(fh)
                    expected_next_chunk = fh.tell() + rlehdr.data_size
                except BadMagicError:
                    rlehdr = dump_empty_bitmap(fh)  # actually returns an ebmhdr
                    expected_next_chunk = fh.tell()
            
        # On to next OpaqueChunkHeader.
        fh.seek(hdr.size, os.SEEK_CUR)

        # But... was there an unexpected gap between where we left off
        # reading and where the next chunk begins?
        if (expected_next_chunk is not None and
            fh.tell() != expected_next_chunk):
            problem("Expected next chunk at", expected_next_chunk,
                    "according to", rlehdr,
                    "but it starts at", fh.tell())
            problem("Gap:", fh.tell() - expected_next_chunk)
            gaps += fh.tell() - expected_next_chunk


def process():
    """Process input files."""
    if not _args.files:
        process_one_file(sys.stdin, '(stdin)')
    else:
        for fname in _args.files:
            if fname == '-':
                process_one_file(sys.stdin, '(stdin)')
            else:
                with open(fname) as F:
                    process_one_file(F, fname)
    return 0


def main(argv=None):
    """Argument parsing and last-ditch exception handling.

    See http://www.artima.com/weblogs/viewpost.jsp?thread=4829
    """
    if argv is None:
        argv = sys.argv

    global _pgm
    _pgm = "%s:" % os.path.basename(argv[0])  # colon for easy use by print

    parser = argparse.ArgumentParser(
        description="Fill in program description here!",
        epilog='Type "pydoc %s" for more information.' % _pgm[:-1])
    parser.add_argument('-v', '--verbose', default=0, action='count',
                        help='Debug logging level, 1=info, 2=debug, 3=debug+')
    parser.add_argument('files', nargs=argparse.REMAINDER,
                        help='The input file(s)')

    global _args
    _args = parser.parse_args(argv[1:])

    try:
        return process()
    except AppError as e:
        print >>sys.stderr, _pgm, e
        return 1
    except IOError as e:
        if e.errno != errno.EPIPE:
            print >>sys.stderr, _pgm, "IOError:", e
            traceback.print_exc()
            return 2
        else:
            print >>sys.stderr, "Broken pipe"
            return 1
    except Exception as e:
        print >>sys.stderr, _pgm, "Unhandled exception:", e
        traceback.print_exc()   # always want this for unexpected exceptions
        return 2

if __name__ == '__main__':
    sys.exit(main())
