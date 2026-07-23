#include <libobsensor/ObSensor.hpp>
#include <iostream>
#include <string>

// Helper to convert OBFormat enum to readable string
std::string formatToString(OBFormat fmt) {
    switch (fmt) {
        case OB_FORMAT_RGB:      return "RGB";
        case OB_FORMAT_MJPEG:    return "MJPEG";
        case OB_FORMAT_YUYV:     return "YUYV";
        case OB_FORMAT_NV12:     return "NV12";
        case OB_FORMAT_NV21:     return "NV21";
        case OB_FORMAT_UYVY:     return "UYVY";
        case OB_FORMAT_GRAY:     return "GRAY";
        case OB_FORMAT_Z16:      return "Z16";
        case OB_FORMAT_Y8:       return "Y8";
        case OB_FORMAT_Y10:      return "Y10";
        case OB_FORMAT_Y11:      return "Y11";
        case OB_FORMAT_Y12:      return "Y12";
        case OB_FORMAT_Y14:      return "Y14";
        case OB_FORMAT_Y16:      return "Y16";
        case OB_FORMAT_BGRA:     return "BGRA";
        case OB_FORMAT_COMPRESSED: return "COMPRESSED";
        default:                 return "UNKNOWN(" + std::to_string(fmt) + ")";
    }
}

// Helper to convert OBSensorType enum to readable string
std::string sensorTypeToString(OBSensorType type) {
    switch (type) {
        case OB_SENSOR_COLOR:       return "COLOR";
        case OB_SENSOR_DEPTH:       return "DEPTH";
        case OB_SENSOR_IR:          return "IR";
        case OB_SENSOR_ACCEL:       return "ACCEL";
        case OB_SENSOR_GYRO:        return "GYRO";
        case OB_SENSOR_IR_LEFT:     return "IR_LEFT";
        case OB_SENSOR_IR_RIGHT:    return "IR_RIGHT";
        case OB_SENSOR_CONFIDENCE:  return "CONFIDENCE";
        default:                    return "UNKNOWN(" + std::to_string(type) + ")";
    }
}

int main() try {
    ob::Pipeline pipe;
    auto device = pipe.getDevice();

    std::cout << "Device: " << device->getDeviceInfo()->name() << std::endl;
    std::cout << "Serial: " << device->getDeviceInfo()->serialNumber() << std::endl;
    std::cout << "========================================" << std::endl;

    auto sensorList = device->getSensorList();
    for (uint32_t i = 0; i < sensorList->count(); i++) {
        auto sensor = sensorList->getSensor(i);
        OBSensorType sType = sensor->getType();

        std::cout << "\n[" << sensorTypeToString(sType) << " SENSOR]" << std::endl;
        std::cout << "----------------------------------------" << std::endl;

        auto profiles = sensor->getStreamProfileList();
        if (profiles->count() == 0) {
            std::cout << "  (No profiles available)" << std::endl;
            continue;
        }

        for (uint32_t j = 0; j < profiles->count(); j++) {
            auto p = profiles->getProfile(j);
            auto vp = p->as<ob::VideoStreamProfile>();

            if (vp) {
                std::cout << "  [" << j << "] "
                          << vp->width() << "x" << vp->height()
                          << " @ " << vp->fps() << "fps"
                          << " | Format: " << formatToString(vp->format())
                          << std::endl;
            } else {
                // Non-video profiles (IMU, etc.)
                std::cout << "  [" << j << "] Non-video stream (skipped)" << std::endl;
            }
        }
    }

    std::cout << "\n========================================" << std::endl;
    std::cout << "Total sensors: " << sensorList->count() << std::endl;
    return 0;

} catch (ob::Error& e) {
    std::cerr << "[ORBBEC ERROR] function:" << e.getFunction()
              << " | message:" << e.what()
              << " | status:" << e.getStatus() << std::endl;
    return 1;
}
