#include <libobsensor/ObSensor.hpp>
#include <iostream>
#include <iomanip>

int main() try {
    ob::Pipeline pipe;
    auto device = pipe.getDevice();

    std::cout << "Device: " << device->getDeviceInfo()->name() << std::endl;
    std::cout << "Serial: " << device->getDeviceInfo()->serialNumber() << std::endl;
    std::cout << "========================================" << std::endl;

    auto camParam = pipe.getCameraParam();

    std::cout << std::fixed << std::setprecision(6);

    std::cout << "\n[DEPTH INTRINSICS]" << std::endl;
    std::cout << "  fx = " << camParam.depthIntrinsic.fx << std::endl;
    std::cout << "  fy = " << camParam.depthIntrinsic.fy << std::endl;
    std::cout << "  cx = " << camParam.depthIntrinsic.cx << std::endl;
    std::cout << "  cy = " << camParam.depthIntrinsic.cy << std::endl;

    std::cout << "\n[COLOR INTRINSICS]" << std::endl;
    std::cout << "  fx = " << camParam.colorIntrinsic.fx << std::endl;
    std::cout << "  fy = " << camParam.colorIntrinsic.fy << std::endl;
    std::cout << "  cx = " << camParam.colorIntrinsic.cx << std::endl;
    std::cout << "  cy = " << camParam.colorIntrinsic.cy << std::endl;

    std::cout << "\n[DEPTH->COLOR EXTRINSICS]" << std::endl;
    std::cout << "  rotation (row-major 3x3):" << std::endl;
    for (int r = 0; r < 3; r++) {
        std::cout << "    ";
        for (int c = 0; c < 3; c++) {
            std::cout << camParam.depthToColorRotation[r * 3 + c];
            if (r * 3 + c < 8) std::cout << ", ";
        }
        std::cout << std::endl;
    }
    std::cout << "  translation (x, y, z):" << std::endl;
    std::cout << "    " << camParam.depthToColorTranslation[0]
              << ", " << camParam.depthToColorTranslation[1]
              << ", " << camParam.depthToColorTranslation[2]
              << std::endl;

    std::cout << "\n========================================" << std::endl;

    // Also print in Python tuple format for easy copy-paste
    std::cout << "\n--- COPY-PASTE FOR camera_pipeline.py ---" << std::endl;
    std::cout << "_DEPTH_FX = " << camParam.depthIntrinsic.fx << std::endl;
    std::cout << "_DEPTH_FY = " << camParam.depthIntrinsic.fy << std::endl;
    std::cout << "_DEPTH_CX = " << camParam.depthIntrinsic.cx << std::endl;
    std::cout << "_DEPTH_CY = " << camParam.depthIntrinsic.cy << std::endl;
    std::cout << "_COLOR_FX_BASE = " << camParam.colorIntrinsic.fx << std::endl;
    std::cout << "_COLOR_FY_BASE = " << camParam.colorIntrinsic.fy << std::endl;
    std::cout << "_COLOR_CX_BASE = " << camParam.colorIntrinsic.cx << std::endl;
    std::cout << "_COLOR_CY_BASE = " << camParam.colorIntrinsic.cy << std::endl;
    std::cout << "_DEPTH_TO_COLOR_ROTATION = (" << std::endl;
    for (int r = 0; r < 3; r++) {
        std::cout << "    ";
        for (int c = 0; c < 3; c++) {
            std::cout << camParam.depthToColorRotation[r * 3 + c];
            if (r * 3 + c < 8) std::cout << ", ";
        }
        std::cout << "," << std::endl;
    }
    std::cout << ")" << std::endl;
    std::cout << "_DEPTH_TO_COLOR_TRANSLATION = ("
              << camParam.depthToColorTranslation[0] << ", "
              << camParam.depthToColorTranslation[1] << ", "
              << camParam.depthToColorTranslation[2] << ")"
              << std::endl;

    return 0;

} catch (ob::Error& e) {
    std::cerr << "[ORBBEC ERROR] function:" << e.getFunction()
              << " | message:" << e.what()
              << " | status:" << e.getStatus() << std::endl;
    return 1;
}
