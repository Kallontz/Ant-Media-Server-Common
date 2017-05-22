package io.antmedia.muxer;

import org.red5.server.api.IConnection;
import org.red5.server.api.scheduling.IScheduledJob;
import org.red5.server.api.scheduling.ISchedulingService;
import org.red5.server.api.scope.IScope;
import org.red5.server.api.stream.IBroadcastStream;
import org.red5.server.api.stream.IStreamPacket;
import org.red5.server.net.rtmp.event.CachedEvent;
import org.red5.server.scheduling.QuartzSchedulingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SuccessCallback;

import com.google.common.collect.EvictingQueue;

import static org.bytedeco.javacpp.avcodec.*;
import static org.bytedeco.javacpp.avformat.AVFMT_GLOBALHEADER;
import static org.bytedeco.javacpp.avformat.AVFMT_NOFILE;
import static org.bytedeco.javacpp.avformat.AVIO_FLAG_WRITE;
import static org.bytedeco.javacpp.avformat.av_read_frame;
import static org.bytedeco.javacpp.avformat.av_write_frame;
import static org.bytedeco.javacpp.avformat.av_write_trailer;
import static org.bytedeco.javacpp.avformat.avformat_alloc_output_context2;
import static org.bytedeco.javacpp.avformat.avformat_find_stream_info;
import static org.bytedeco.javacpp.avformat.avformat_free_context;
import static org.bytedeco.javacpp.avformat.avformat_new_stream;
import static org.bytedeco.javacpp.avformat.avformat_open_input;
import static org.bytedeco.javacpp.avformat.avformat_write_header;
import static org.bytedeco.javacpp.avformat.avio_alloc_context;
import static org.bytedeco.javacpp.avformat.avio_closep;

import java.io.File;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.IntPointer;
import org.bytedeco.javacpp.Pointer;
import org.bytedeco.javacpp.avformat;
import org.bytedeco.javacpp.avutil;

import static org.bytedeco.javacpp.avutil.*;

import org.bytedeco.javacpp.avcodec.AVBSFContext;
import org.bytedeco.javacpp.avcodec.AVBitStreamFilter;
import org.bytedeco.javacpp.avcodec.AVBitStreamFilterContext;
import org.bytedeco.javacpp.avcodec.AVCodec;
import org.bytedeco.javacpp.avcodec.AVCodecContext;
import org.bytedeco.javacpp.avcodec.AVCodecParameters;
import org.bytedeco.javacpp.avcodec.AVPacket;
import org.bytedeco.javacpp.avformat.AVFormatContext;
import org.bytedeco.javacpp.avformat.AVIOContext;
import org.bytedeco.javacpp.avformat.AVInputFormat;
import org.bytedeco.javacpp.avformat.AVStream;
import org.bytedeco.javacpp.avformat.Read_packet_Pointer_BytePointer_int;
import org.bytedeco.javacpp.avutil.AVDictionary;
import org.bytedeco.javacpp.avutil.AVRational;


public class HLSMuxer extends Muxer  {


	private AVBitStreamFilter h264bsfc;
	private AVBSFContext bsfContext;
	private long lastDTS = -1; 

	private List<Integer> registeredStreamIndexList = new ArrayList<>();

	protected static Logger logger = LoggerFactory.getLogger(HLSMuxer.class);
	public final static String  HLS_LIST_SIZE = "5";
	public final static String HLS_TIME = "2";

	private AVRational avRationalTimeBase;
	private long totalSize;
	private long partialTotalSize;
	private long startTime;
	private long currentTime;
	private long bitrate;
	private long bitrateReferenceTime;
	
	int videoWidth;
	int videoHeight;
	

	public HLSMuxer() {
		extension = ".m3u8";
		format = "hls";
		options.put("hls_list_size", HLS_LIST_SIZE);
		options.put("hls_time", HLS_TIME);
		options.put("hls_flags", "delete_segments");
		
		avRationalTimeBase = new AVRational();
		avRationalTimeBase.num(1);
		avRationalTimeBase.den(1);
		
	}
	
	private AVFormatContext getOutputFormatContext() {
		if (outputFormatContext == null) {
			h264bsfc = av_bsf_get_by_name("h264_mp4toannexb");
			bsfContext = new AVBSFContext(null);
			
			int ret = av_bsf_alloc(h264bsfc, bsfContext);
			if (ret < 0) {
				logger.info("cannot allocate bsf context");
				return null;
			}
			
			outputFormatContext= new AVFormatContext(null);
			ret = avformat_alloc_output_context2(outputFormatContext, null, format, file.getAbsolutePath());
			if (ret < 0) {
				logger.info("Could not create output context\n");
				return null;
			}
		}
		return outputFormatContext;
	}

	public boolean prepare(AVFormatContext inputFormatContext) {

		AVFormatContext context = getOutputFormatContext();

		for (int i=0; i < inputFormatContext.nb_streams(); i++) {
			AVStream in_stream = inputFormatContext.streams(i);
			if (isCodecSupported(in_stream.codecpar().codec_id())) {
				registeredStreamIndexList.add(i);
				AVStream out_stream = avformat_new_stream(context, in_stream.codec().codec());

				if (in_stream.codec().codec_type() == AVMEDIA_TYPE_VIDEO) {
					int ret = avcodec_parameters_copy(bsfContext.par_in(), in_stream.codecpar());
					if (ret < 0) {
						logger.info("cannot copy input codec parameters");
						return false;
					}
					bsfContext.time_base_in(in_stream.time_base());

					ret = av_bsf_init(bsfContext);
					if (ret < 0) {
						logger.info("cannot init bit stream filter context");
						return false;
					}

					ret = avcodec_parameters_copy(out_stream.codecpar(), bsfContext.par_out());
					if (ret < 0) {
						logger.info("cannot copy codec parameters to output");
						return false;
					}
					videoWidth = out_stream.codecpar().width();
					videoHeight = out_stream.codecpar().height();
					out_stream.time_base(bsfContext.time_base_out());
				}
				else {

					int ret = avcodec_parameters_copy(out_stream.codecpar(), in_stream.codecpar());
					if (ret < 0) {
						logger.info("Cannot get codec parameters\n");
						return false;
					}

				}
				out_stream.codec().codec_tag(0);

				if ((context.oformat().flags() & AVFMT_GLOBALHEADER) != 0)
					out_stream.codec().flags( out_stream.codec().flags() | AV_CODEC_FLAG_GLOBAL_HEADER);
			}
		}

		prepareIO();

		return true;
	}

	private boolean isCodecSupported(int codecId) {
		if (codecId == AV_CODEC_ID_H264 || 
				codecId == AV_CODEC_ID_AAC) {
			return true;
		}
		return false;
	}
	
	/**
	 * 
	 * @return the bitrate in last 1 second
	 */
	public long getBitrate() {
		return bitrate;
	}
	
	public long getAverageBitrate() {

		long duration = (currentTime - startTime) ;
		
		if (duration > 0) 
		{
			return (totalSize / duration) * 8;
		}
		return 0;
	}

	
	public void writePacket(AVPacket pkt, AVRational inputTimebase, AVRational outputTimebase, int codecType) 
	{
		int packetIndex = pkt.stream_index();
		
		if (!registeredStreamIndexList.contains(packetIndex))  {
			return;
		}
		
	
		
		long pts = pkt.pts();
		long dts = pkt.dts();
		long duration = pkt.duration();
		long pos = pkt.pos();
		
		totalSize += pkt.size();
		partialTotalSize += pkt.size();
		currentTime = av_rescale_q(dts, inputTimebase, avRationalTimeBase);
		if (startTime == 0) {
			startTime = currentTime;
			bitrateReferenceTime = currentTime;
		}
	
		if ((currentTime - bitrateReferenceTime) >= 1) {
			bitrate = partialTotalSize * 8;
			partialTotalSize = 0;
			bitrateReferenceTime = currentTime;
		}
		
		int ret;
		if (codecType ==  AVMEDIA_TYPE_VIDEO) 
		{

			ret = av_bsf_send_packet(bsfContext, pkt);
			if (ret < 0)
				return;

			while ((ret = av_bsf_receive_packet(bsfContext, pkt)) == 0) 
			{
				
				pkt.pts(av_rescale_q_rnd(pkt.pts(), inputTimebase, outputTimebase, AV_ROUND_NEAR_INF|AV_ROUND_PASS_MINMAX));
				pkt.dts(av_rescale_q_rnd(pkt.dts(), inputTimebase, outputTimebase, AV_ROUND_NEAR_INF|AV_ROUND_PASS_MINMAX));
				pkt.duration(av_rescale_q(pkt.duration(), inputTimebase, outputTimebase));
				pkt.pos(-1);

				ret = av_write_frame(outputFormatContext, pkt);
				if (ret < 0) {
					logger.info("cannot write frame to muxer");
				}
				
				pkt.pts(pts);
				pkt.dts(dts);
				pkt.duration(duration);
				pkt.pos(pos);
				
			}
		}
		else {
			
			
			pkt.pts(av_rescale_q_rnd(pkt.pts(), inputTimebase, outputTimebase, AV_ROUND_NEAR_INF|AV_ROUND_PASS_MINMAX));
			pkt.dts(av_rescale_q_rnd(pkt.dts(), inputTimebase, outputTimebase, AV_ROUND_NEAR_INF|AV_ROUND_PASS_MINMAX));
			pkt.duration(av_rescale_q(pkt.duration(), inputTimebase, outputTimebase));
			pkt.pos(-1);
			
			ret = av_write_frame(outputFormatContext, pkt);
			if (ret < 0) {
				logger.info("cannot write frame to muxer");
			}
			
			pkt.pts(pts);
			pkt.dts(dts);
			pkt.duration(duration);
			pkt.pos(pos);
		}

	}


	@Override
	public void writeTrailer() {
		av_bsf_free(bsfContext);

		av_write_trailer(outputFormatContext);

		/* close output */
		if ((outputFormatContext.flags() & AVFMT_NOFILE) == 0)
			avio_closep(outputFormatContext.pb());

		avformat_free_context(outputFormatContext);
		
		outputFormatContext = null;
		
		isRecording = false;	
	}


	@Override
	public void writePacket(AVPacket pkt) {
		AVStream out_stream = outputFormatContext.streams(pkt.stream_index());
		writePacket(pkt, out_stream.codec().time_base(),  out_stream.time_base(), out_stream.codecpar().codec_type()); 
		
	}

	@Override
	public boolean addStream(AVCodec codec, AVCodecContext codecContext, int streamIndex) {
		
		AVFormatContext context = getOutputFormatContext();

		if (context == null) {
			return false;
		}
		if (isCodecSupported(codecContext.codec_id())) {
			registeredStreamIndexList.add(streamIndex);
			AVStream out_stream = avformat_new_stream(context, codec);
			out_stream.index(streamIndex);
			if (codecContext.codec_type() == AVMEDIA_TYPE_VIDEO) {
				int ret = avcodec_parameters_from_context(bsfContext.par_in(), codecContext);
				//int ret = avcodec_parameters_copy(bsfContext.par_in(), in_stream.codecpar());
				if (ret < 0) {
					logger.info("cannot copy input codec parameters");
					return false;
				}
				//TODO: do we need to set codec context, it may cause some memory problems if 
				//codec context is used in a same way with another muxer
				//out_stream.codec(codecContext);
				out_stream.codec().time_base(codecContext.time_base());
				bsfContext.time_base_in(codecContext.time_base());

				ret = av_bsf_init(bsfContext);
				if (ret < 0) {
					logger.info("cannot init bit stream filter context");
					return false;
				}

				ret = avcodec_parameters_copy(out_stream.codecpar(), bsfContext.par_out());
				if (ret < 0) {
					logger.info("cannot copy codec parameters to output");
					return false;
				}
				videoWidth = codecContext.width();
				videoHeight = codecContext.height();

				out_stream.time_base(bsfContext.time_base_out());
			}
			else {
				//TODO: do we need this setting codec context, it may cause some memory problems if 
				//codec context is used in a same way with another muxer
				out_stream.codec().time_base(codecContext.time_base());
				int ret = avcodec_parameters_from_context(out_stream.codecpar(), codecContext);
				
				//out_stream.codec(codecContext);
			}
			out_stream.codec().codec_tag(0);

			if ((context.oformat().flags() & AVFMT_GLOBALHEADER) != 0)
				out_stream.codec().flags( out_stream.codec().flags() | AV_CODEC_FLAG_GLOBAL_HEADER);


		}
		return true;
	}


	@Override
	public boolean prepareIO() {
		AVIOContext pb = new AVIOContext(null);
		
		int ret = avformat.avio_open(pb,  file.getAbsolutePath(), AVIO_FLAG_WRITE);
		if (ret < 0) {
			logger.warn("Could not open output file");
			return false;
		}
		getOutputFormatContext().pb(pb);

		AVDictionary optionsDictionary = null;

		if (!options.isEmpty()) {
			optionsDictionary = new AVDictionary();
			Set<String> keySet = options.keySet();
			for (String key : keySet) {
				av_dict_set(optionsDictionary, key, options.get(key), 0);
			}
		}
		ret = avformat_write_header(getOutputFormatContext(), optionsDictionary);		
		if (ret < 0) {
			logger.warn("could not write header");
			return false;
		}
		
		if (optionsDictionary != null) {
			av_dict_free(optionsDictionary);
		}
		return true;
	}

	@Override
	public void writePacket(AVPacket avpacket, AVStream inStream) {
		AVStream out_stream = getOutputFormatContext().streams(avpacket.stream_index());
		writePacket(avpacket, inStream.time_base(),  out_stream.time_base(), inStream.codecpar().codec_type()); 
		
	}
	
	public int getVideoWidth() {
		return videoWidth;
	}
	
	public int getVideoHeight() {
		return videoHeight;
	}



}