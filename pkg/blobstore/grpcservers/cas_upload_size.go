package grpcservers

import (
	"github.com/buildbarn/bb-storage/pkg/digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func checkCASUploadSize(d digest.Digest, maximumSizeBytes int64) error {
	if maximumSizeBytes > 0 && d.GetSizeBytes() > maximumSizeBytes {
		return status.Errorf(codes.InvalidArgument, "CAS blob size %d exceeds maximum upload size %d", d.GetSizeBytes(), maximumSizeBytes)
	}
	return nil
}
