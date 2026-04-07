import { useEffect } from "react";
import { X } from "lucide-react";

import { Button } from "@/components/ui/button";

type ProfileDropModalProps = {
  isOpen: boolean;
  onClose: () => void;
  profilePathUrl: string | null | undefined;
  candidateName: string;
};

export function ProfileDropModal({
  isOpen,
  onClose,
  profilePathUrl,
  candidateName,
}: ProfileDropModalProps) {
  useEffect(() => {
    if (!isOpen) return;

    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        onClose();
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    document.body.style.overflow = "hidden";

    return () => {
      document.removeEventListener("keydown", handleKeyDown);
      document.body.style.overflow = "";
    };
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center"
      onClick={onClose}
    >
      <div className="absolute inset-0 bg-black/80 backdrop-blur-sm" />
      <div className="relative z-10 flex max-h-[90vh] max-w-[90vw] flex-col items-center justify-center">
        <div className="absolute -top-12 right-0 flex items-center gap-4">
          <span className="text-sm text-white/70">{candidateName}</span>
          <Button
            variant="ghost"
            size="icon"
            onClick={onClose}
            className="text-white/70 hover:text-white hover:bg-white/10"
          >
            <X className="h-5 w-5" />
          </Button>
        </div>
        <div
          className="flex max-h-[85vh] max-w-[90vw] items-center justify-center overflow-hidden rounded-2xl border border-white/10 bg-black/40"
          onClick={(e) => e.stopPropagation()}
        >
          {profilePathUrl ? (
            <img
              src={profilePathUrl}
              alt={candidateName}
              className="max-h-[85vh] max-w-[90vw] object-contain"
            />
          ) : (
            <div className="flex h-96 w-96 items-center justify-center text-white/50">
              <div className="text-center">
                <p className="text-lg font-medium">Profile drop not available</p>
                <p className="mt-2 text-sm text-white/30">
                  No PNG has been rendered for this attempt.
                </p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}